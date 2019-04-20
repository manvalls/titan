package cinode

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/manvalls/fuse/fuseops"
	"github.com/manvalls/titan/database"
	"github.com/manvalls/titan/math"
	"github.com/manvalls/titan/storage"
)

// Inode represents a cached inode
type Inode struct {
	Chunks  []database.Chunk
	Size    uint64
	Storage storage.Storage
	Path    string
	Inode   fuseops.InodeID

	MaxOffsetDistance uint64
	InactivityTimeout time.Duration
	ReadAheadTimeout  time.Duration
	BufferSize        uint32
	Atime             time.Time
	Ctime             time.Time
	LastValidation    time.Time

	mutex      sync.Mutex
	sections   []*section
	file       *os.File
	fetchError error
}

type section struct {
	size       uint64
	offset     uint64
	listeners  []listener
	active     bool
	lastActive time.Time
}

type listener struct {
	size    uint64
	offset  uint64
	channel chan error
}

var errIsClosed = errors.New("inode closed")

// NewInode creates a new inode
func NewInode() *Inode {
	return &Inode{
		Atime:             time.Now(),
		LastValidation:    time.Now(),
		BufferSize:        65536,
		MaxOffsetDistance: 100e3,
		InactivityTimeout: 20 * time.Second,
		ReadAheadTimeout:  100 * time.Millisecond,
		mutex:             sync.Mutex{},
		sections:          make([]*section, 0),
	}
}

// Close closes this inode
func (inode *Inode) Close() {
	inode.sendError(errIsClosed)
}

// ReadAt reads into the provided buffer at the provided offset
func (inode *Inode) ReadAt(b []byte, off int64) (n int, err error) {
	if uint64(off) < inode.Size {
		endPosition := math.Min(inode.Size, uint64(off+int64(len(b))))
		err = <-inode.wait(uint64(off), endPosition-uint64(off))
		if err != nil {
			return 0, err
		}
	}

	file, err := inode.getFile()
	if err != nil {
		return 0, err
	}

	return file.ReadAt(b, off)
}

func (inode *Inode) getFile() (*os.File, error) {
	inode.mutex.Lock()
	defer inode.mutex.Unlock()

	if inode.fetchError != nil {
		return nil, inode.fetchError
	}

	inode.Atime = time.Now()

	if inode.file != nil {
		return inode.file, nil
	}

	file, err := os.OpenFile(inode.Path, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}

	inode.file = file

	go func() {

		for {

			inode.mutex.Lock()

			if inode.file != file {
				inode.mutex.Unlock()
				return
			}

			inactivity := time.Now().Sub(inode.Atime)

			if inactivity >= inode.InactivityTimeout {
				inode.file.Close()
				inode.file = nil
				inode.mutex.Unlock()
				return
			}

			inode.mutex.Unlock()
			time.Sleep(inode.InactivityTimeout - inactivity)

		}

	}()

	return file, nil
}

func (inode *Inode) cached(offset, size uint64) bool {
	for _, section := range inode.sections {
		if offset < section.offset {
			return false
		}

		if offset >= section.offset && offset+size <= section.offset+section.size {
			return true
		}
	}

	return false
}

func (inode *Inode) getSection(offset uint64) *section {
	for _, section := range inode.sections {
		if offset < section.offset {
			return nil
		}

		correctedOffset := offset
		if offset >= inode.MaxOffsetDistance {
			correctedOffset -= inode.MaxOffsetDistance
		}

		if correctedOffset <= section.offset+section.size {
			return section
		}
	}

	return nil
}

func (inode *Inode) wait(offset, size uint64) <-chan error {
	inode.mutex.Lock()
	defer inode.mutex.Unlock()

	channel := make(chan error, 1)

	var s *section

	if inode.fetchError != nil {
		channel <- inode.fetchError
	} else if inode.cached(offset, size) {
		s = inode.getSection(offset)
		channel <- nil
	} else {
		s = inode.getSection(offset)
		if s == nil {
			s = &section{
				offset:    offset,
				listeners: make([]listener, 0),
			}

			inode.addSection(s)
		}

		s.listeners = append(s.listeners, listener{
			offset:  offset,
			size:    size,
			channel: channel,
		})
	}

	if s != nil {
		s.lastActive = time.Now()
		if !s.active {
			s.active = true
			go inode.fetch(s.offset + s.size)
		}
	}

	return channel
}

func (inode *Inode) addSection(s *section) {
	var prevSection *section
	newSections := make([]*section, 0)
	inserted := false

	for _, section := range inode.sections {

		if !inserted && (prevSection == nil || prevSection.offset+prevSection.size < s.offset) && s.offset+s.size < section.offset {
			newSections = append(newSections, s)
			inserted = true
		}

		if section.offset > s.offset+s.size || section.offset+section.size < s.offset {
			prevSection = section
			newSections = append(newSections, section)
		}

	}

	if !inserted {
		newSections = append(newSections, s)
	}

	inode.sections = newSections
}

func (s *section) checkListeners() {
	if len(s.listeners) > 0 {
		s.lastActive = time.Now()
		newListeners := make([]listener, 0)

		for _, listener := range s.listeners {
			if s.offset <= listener.offset && s.offset+s.size >= listener.size+listener.offset {
				listener.channel <- nil
			} else {
				newListeners = append(newListeners, listener)
			}
		}

		s.listeners = newListeners
	}
}

func (inode *Inode) addRange(offset, size uint64) bool {
	inode.mutex.Lock()
	defer inode.mutex.Unlock()

	newSection := &section{
		offset:    offset,
		size:      size,
		active:    true,
		listeners: make([]listener, 0),
	}

	lastSectionActive := true

	for _, section := range inode.sections {
		if section.offset <= offset+size && section.offset+section.size >= offset {
			to := math.Max(newSection.offset+newSection.size, section.offset+section.size)
			newSection.offset = math.Min(section.offset, newSection.offset)
			newSection.size = to - newSection.offset
			newSection.listeners = append(newSection.listeners, section.listeners...)

			if section.lastActive.After(newSection.lastActive) {
				newSection.lastActive = section.lastActive
			}

			lastSectionActive = section.active
		}
	}

	inode.addSection(newSection)
	newSection.checkListeners()

	if len(newSection.listeners) == 0 && newSection.lastActive.Add(inode.ReadAheadTimeout).Before(time.Now()) {
		newSection.active = false
		return true
	}

	if newSection.offset+newSection.size > offset+size {
		if !lastSectionActive {
			go inode.fetch(newSection.offset + newSection.size)
		}

		return true
	}

	return false
}

func (inode *Inode) sendError(err error) {
	inode.mutex.Lock()
	defer inode.mutex.Unlock()

	if inode.file != nil {
		inode.file.Close()
		inode.file = nil
	}

	os.Remove(inode.Path)

	inode.fetchError = err

	for _, s := range inode.sections {
		listeners := s.listeners
		s.listeners = make([]listener, 0)

		for _, listener := range listeners {
			listener.channel <- err
		}
	}
}

func (inode *Inode) fetch(offset uint64) {
	buffer := make([]byte, inode.BufferSize)

	for _, chunk := range inode.Chunks {
		if chunk.InodeOffset+chunk.Size > offset {
			var err error

			inode.mutex.Lock()
			if inode.fetchError != nil {
				inode.mutex.Unlock()
				return
			}

			inode.mutex.Unlock()

			storageChunk := storage.Chunk{
				Key:          chunk.Key,
				ObjectOffset: chunk.ObjectOffset,
				Size:         chunk.Size,
				Storage:      chunk.Storage,
			}

			if chunk.InodeOffset < offset {
				delta := offset - chunk.InodeOffset
				storageChunk.ObjectOffset += delta
				storageChunk.Size -= delta
			}

			reader, err := inode.Storage.GetReadCloser(storageChunk)

			if err != nil {
				inode.sendError(err)
				return
			}

			defer reader.Close()

			for {
				n, readErr := reader.Read(buffer)
				if readErr != nil && readErr != io.EOF {
					inode.sendError(readErr)
					return
				}

				file, fileErr := inode.getFile()
				if fileErr != nil {
					inode.sendError(fileErr)
					return
				}

				_, err = file.WriteAt(buffer[:n], int64(offset))
				if err != nil {
					inode.sendError(err)
					return
				}

				if inode.addRange(offset, uint64(n)) {
					return
				}

				offset += uint64(n)

				if readErr == io.EOF {
					break
				}
			}
		}
	}

	inode.addRange(offset, inode.Size)
}
