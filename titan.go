package titan

import (
	"time"

	"github.com/manvalls/fuse"
	"github.com/manvalls/fuse/fuseutil"
	"github.com/manvalls/titan/cache"
	"github.com/manvalls/titan/database"
	"github.com/manvalls/titan/filesystem"
	"github.com/manvalls/titan/storage"
)

// MountOptions holds several mount options
type MountOptions struct {
	storage.Storage
	database.Db
	CacheLocation string

	*fuse.MountConfig
	PruneInterval        *time.Duration
	InactivityTimeout    *time.Duration
	ReadAheadTimeout     *time.Duration
	CtimeCacheTimeout    *time.Duration
	FreeSpaceThreshold   *uint64
	MaxInodes            *uint64
	BufferSize           *uint32
	MaxOffsetDistance    *uint64
	AttributesExpiration *time.Duration
	EntryExpiration      *time.Duration
	MaxChunkSize         *int64
	AsyncFlush           *bool
	EnableCapabilities   *bool
}

// Mount mounts the titan file system with the provided options
func Mount(dir string, opt MountOptions) (mfs *fuse.MountedFileSystem, err error) {
	c := cache.NewCache()
	c.Db = opt.Db
	c.Storage = opt.Storage
	c.CacheLocation = opt.CacheLocation

	if opt.PruneInterval != nil {
		c.PruneInterval = *opt.PruneInterval
	}

	if opt.InactivityTimeout != nil {
		c.InactivityTimeout = *opt.InactivityTimeout
	}

	if opt.ReadAheadTimeout != nil {
		c.ReadAheadTimeout = *opt.ReadAheadTimeout
	}

	if opt.CtimeCacheTimeout != nil {
		c.CtimeCacheTimeout = *opt.CtimeCacheTimeout
	}

	if opt.BufferSize != nil {
		c.BufferSize = *opt.BufferSize
	}

	if opt.MaxOffsetDistance != nil {
		c.MaxOffsetDistance = *opt.MaxOffsetDistance
	}

	if opt.FreeSpaceThreshold != nil {
		c.FreeSpaceThreshold = *opt.FreeSpaceThreshold
	}

	if opt.MaxInodes != nil {
		c.MaxInodes = *opt.MaxInodes
	}

	err = c.Init()

	if err != nil {
		return
	}

	fs := filesystem.NewFileSystem()
	fs.Db = opt.Db
	fs.Storage = opt.Storage
	fs.Cache = c

	if opt.AttributesExpiration != nil {
		fs.AttributesExpiration = *opt.AttributesExpiration
	}

	if opt.EntryExpiration != nil {
		fs.EntryExpiration = *opt.EntryExpiration
	}

	if opt.MaxChunkSize != nil {
		fs.MaxChunkSize = *opt.MaxChunkSize
	}

	if opt.AsyncFlush != nil {
		fs.AsyncFlush = *opt.AsyncFlush
	}

	if opt.EnableCapabilities != nil {
		fs.EnableCapabilities = *opt.EnableCapabilities
	}

	return fuse.Mount(dir, fuseutil.NewFileSystemServer(fs), opt.MountConfig)
}
