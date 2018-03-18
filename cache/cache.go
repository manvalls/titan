package cache

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"git.vlrz.es/manvalls/titan/cache/cinode"
	"git.vlrz.es/manvalls/titan/database"
	"git.vlrz.es/manvalls/titan/storage"
	"github.com/manvalls/fuse/fuseops"
)

// Cache abstracts away file I/O through a local cache
type Cache struct {
	database.Db
	storage.Storage
	CacheLocation      string
	PruneInterval      time.Duration
	InactivityTimeout  time.Duration
	CtimeCacheTimeout  time.Duration
	FreeSpaceThreshold uint64
	MaxInodes          uint64
	BufferSize         uint32

	stopChannel chan bool
	mutex       sync.Mutex
	inodes      map[fuseops.InodeID]*cinode.Inode
}

// NewCache returns a new local cache
func NewCache() *Cache {
	return &Cache{
		PruneInterval:      5 * time.Minute,
		InactivityTimeout:  20 * time.Second,
		CtimeCacheTimeout:  60 * time.Second,
		BufferSize:         15e3,
		FreeSpaceThreshold: 5 * 1e9,
		MaxInodes:          10e3,
		stopChannel:        make(chan bool),
		mutex:              sync.Mutex{},
		inodes:             make(map[fuseops.InodeID]*cinode.Inode),
	}
}

// Init initialises the local cache
func (c *Cache) Init() error {
	os.RemoveAll(c.CacheLocation)
	err := os.MkdirAll(c.CacheLocation, 0777)
	if err != nil {
		return err
	}

	go func() {

		for {
			select {
			case <-c.stopChannel:
				return
			case <-time.After(c.PruneInterval):
				c.prune()
			}
		}

	}()

	return nil
}

// Destroy destroys this cache instance
func (c *Cache) Destroy() error {
	c.stopChannel <- true
	return os.RemoveAll(c.CacheLocation)
}

// Validate checks a certain entry for its validity
func (c *Cache) Validate(inode fuseops.InodeID) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	in, ok := c.inodes[inode]
	if !ok {
		return
	}

	dbInode, err := c.Db.Get(context.Background(), inode)
	if err != nil {
		c.rm(inode)
		return
	}

	if in.Ctime.Before(dbInode.Ctime) {
		c.rm(inode)
	} else {
		in.LastValidation = time.Now()
	}
}

// ReadInodeAt fills the provided buffer for the provided inode at the
// provided offset
func (c *Cache) ReadInodeAt(inode fuseops.InodeID, p []byte, off int64) (n int, err error) {
	c.mutex.Lock()

	in, ok := c.inodes[inode]

	if ok && time.Now().Sub(in.LastValidation) > c.CtimeCacheTimeout {
		c.Validate(inode)
		in, ok = c.inodes[inode]
	}

	if !ok {
		var chunks *[]database.Chunk
		var key string
		var dbInode *database.Inode

		dbInode, err = c.Db.Get(context.Background(), inode)
		if err != nil {
			c.mutex.Unlock()
			return 0, err
		}

		in = cinode.NewInode()
		chunks, err = c.Db.Chunks(context.Background(), inode)
		if err != nil {
			c.mutex.Unlock()
			return 0, err
		}

		in.Chunks = *chunks
		in.Size = dbInode.Size
		in.Ctime = dbInode.Ctime
		in.Storage = c.Storage

		key, err = storage.Key()
		if err != nil {
			c.mutex.Unlock()
			return 0, err
		}

		in.Path = filepath.Join(c.CacheLocation, key)
		in.Inode = inode
		in.InactivityTimeout = c.InactivityTimeout
		in.BufferSize = c.BufferSize
		c.inodes[inode] = in
	}

	c.mutex.Unlock()
	return in.ReadAt(p, off)
}

// Rm removes an entry from the cache
func (c *Cache) Rm(inode fuseops.InodeID) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.rm(inode)
}

func (c *Cache) rm(inode fuseops.InodeID) {
	in, ok := c.inodes[inode]
	if !ok {
		return
	}

	os.Remove(in.Path)
	delete(c.inodes, inode)
}

func (c *Cache) lenInodes() uint64 {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return uint64(len(c.inodes))
}

func (c *Cache) inodesSlice() []*cinode.Inode {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	inodes := make([]*cinode.Inode, 0, len(c.inodes))
	for _, inode := range c.inodes {
		inodes = append(inodes, inode)
	}

	return inodes
}

func (c *Cache) shouldPrune() bool {
	inodesLen := c.lenInodes()
	if inodesLen == 0 {
		return false
	}

	stats := syscall.Statfs_t{}
	err := syscall.Statfs(c.CacheLocation, &stats)
	if err != nil {
		return false
	}

	freeSpace := stats.Bavail * uint64(stats.Bsize)
	if uint64(inodesLen) < c.MaxInodes && freeSpace > c.FreeSpaceThreshold {
		return false
	}

	return true
}

func (c *Cache) prune() {
	if !c.shouldPrune() {
		return
	}

	inodes := c.inodesSlice()

	for len(inodes) > 0 && c.shouldPrune() {

		sort.Sort(byAtime(inodes))

		if len(inodes) == 1 {
			c.Rm(inodes[0].Inode)
			return
		}

		candidates := make([]*cinode.Inode, 0, len(inodes)/2)
		newInodes := make([]*cinode.Inode, 0, len(inodes)/2)

		for i, inode := range inodes {
			if i < len(inodes)/2 {
				candidates = append(candidates, inode)
			} else {
				newInodes = append(newInodes, inode)
			}
		}

		sort.Sort(bySize(candidates))

		if len(candidates) == 1 {
			c.Rm(candidates[0].Inode)
		} else {
			for i, inode := range candidates {
				if i < len(inodes)/2 {
					c.Rm(inode.Inode)
				} else {
					newInodes = append(newInodes, inode)
				}
			}
		}

		inodes = newInodes

	}
}

type byAtime []*cinode.Inode

func (s byAtime) Len() int {
	return len(s)
}

func (s byAtime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byAtime) Less(i, j int) bool {
	return s[i].Atime.Before(s[j].Atime)
}

type bySize []*cinode.Inode

func (s bySize) Len() int {
	return len(s)
}

func (s bySize) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s bySize) Less(i, j int) bool {
	return s[i].Size < s[j].Size
}
