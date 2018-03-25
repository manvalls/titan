package database

import (
	"context"
	"os"
	"time"

	"github.com/manvalls/fuse/fuseops"
	"github.com/manvalls/titan/storage"
)

// Db contains methods for interacting with
// the underlying database
type Db interface {
	Open() error
	Close() error

	Setup(ctx context.Context) error
	Stats(ctx context.Context) (*Stats, error)
	Create(ctx context.Context, entry Entry) (*Entry, error)
	Forget(ctx context.Context, inode fuseops.InodeID) error
	CleanOrphanInodes(ctx context.Context) error
	CleanOrphanChunks(ctx context.Context, threshold time.Time, st storage.Storage, workers int) error

	Unlink(ctx context.Context, parent fuseops.InodeID, name string) error
	Rename(ctx context.Context, oldParent fuseops.InodeID, oldName string, newParent fuseops.InodeID, newName string) error

	LookUp(ctx context.Context, parent fuseops.InodeID, name string) (*Entry, error)
	Get(ctx context.Context, inode fuseops.InodeID) (*Inode, error)
	Touch(ctx context.Context, inode fuseops.InodeID, size *uint64, mode *os.FileMode, atime *time.Time, mtime *time.Time, uid *uint32, gid *uint32) (*Inode, error)

	AddChunk(ctx context.Context, inode fuseops.InodeID, chunk Chunk) error
	Chunks(ctx context.Context, inode fuseops.InodeID) (*[]Chunk, error)
	Children(ctx context.Context, inode fuseops.InodeID) (*[]Child, error)

	ListXattr(ctx context.Context, inode fuseops.InodeID) (*[]string, error)
	RemoveXattr(ctx context.Context, inode fuseops.InodeID, attr string) error
	GetXattr(ctx context.Context, inode fuseops.InodeID, attr string) (*[]byte, error)
	SetXattr(ctx context.Context, inode fuseops.InodeID, attr string, value []byte, flags uint32) error
}

// Entry represents an entry of the file system
type Entry struct {
	Parent fuseops.InodeID
	Name   string
	Inode
}

// Inode represents a file system inode
type Inode struct {
	ID      fuseops.InodeID
	SymLink string
	fuseops.InodeAttributes
}

// Stats contain information about file system usage
type Stats struct {
	Inodes uint64
	Size   uint64
}

// Chunk contains information about the location of a particular piece
// of binary data
type Chunk struct {
	// ID represents the id of this chunk within the database
	ID uint64

	// Inode points to the owner inode of this chunk
	Inode fuseops.InodeID

	// InodeOffset represents the first byte of the inode that the first byte of
	// this chunk points to
	InodeOffset uint64

	// Chunk points to the relevant storage chunk
	storage.Chunk
}

// Child represents a child entry within a directory
type Child struct {
	Inode fuseops.InodeID
	Name  string
	Mode  os.FileMode
}
