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
	BufferSize        uint32
	Atime             time.Time
	Ctime             time.Time
	LastValidation    time.Time

	mutex      sync.Mutex
	listeners  []listener
	sections   []section
	file       *os.File
	fetchError error
}

type section struct {
	size   uint64
	offset uint64
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
		mutex:             sync.Mutex{},
		listeners:         make([]listener, 0),
		sections:          make([]section, 0),
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

func (inode *Inode) shouldFetch(offset uint64) bool {
	for _, section := range inode.sections {
		if offset < section.offset {
			return true
		}

		if offset-inode.MaxOffsetDistance <= section.offset+section.size {
			return false
		}
	}

	return true
}

func (inode *Inode) wait(offset, size uint64) <-chan error {
	inode.mutex.Lock()
	defer inode.mutex.Unlock()

	channel := make(chan error, 1)

	if inode.fetchError != nil {
		channel <- inode.fetchError
	} else if inode.cached(offset, size) {
		channel <- nil
	} else {
		inode.listeners = append(inode.listeners, listener{
			offset:  offset,
			size:    size,
			channel: channel,
		})

		if inode.shouldFetch(offset) {
			go inode.fetch(offset)
		}
	}

	return channel
}

func (inode *Inode) addRange(offset, size uint64) bool {
	inode.mutex.Lock()
	defer inode.mutex.Unlock()

	newSection := section{
		offset: offset,
		size:   size,
	}

	for _, section := range inode.sections {
		if section.offset <= offset+size && section.offset+section.size >= offset {
			to := math.Max(newSection.offset+newSection.size, section.offset+section.size)
			newSection.offset = math.Min(section.offset, newSection.offset)
			newSection.size = to - newSection.offset
		}
	}

	prevSection := section{}
	hasPrevSection := false
	newSections := make([]section, 0)
	newSectionInserted := false

	for _, section := range inode.sections {

		if !newSectionInserted && (!hasPrevSection || prevSection.offset+prevSection.size < newSection.offset) && newSection.offset+newSection.size < section.offset {
			newSections = append(newSections, newSection)
			newSectionInserted = true
		}

		if section.offset > newSection.offset+newSection.size || section.offset+section.size < newSection.offset {
			prevSection = section
			hasPrevSection = true
			newSections = append(newSections, section)
		}

	}

	if !newSectionInserted {
		newSections = append(newSections, newSection)
	}

	inode.sections = newSections
	newListeners := make([]listener, 0)

	for _, listener := range inode.listeners {
		if newSection.offset <= listener.offset && newSection.offset+newSection.size >= listener.size+listener.offset {
			listener.channel <- nil
		} else {
			newListeners = append(newListeners, listener)
		}
	}

	inode.listeners = newListeners
	return newSection.offset+newSection.size > offset+size
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
	listeners := inode.listeners
	inode.listeners = make([]listener, 0)

	for _, listener := range listeners {
		listener.channel <- err
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

}
