package database

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/jacobsa/fuse/fuseops"
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

	Unlink(ctx context.Context, parent fuseops.InodeID, name string, removeDots bool) error
	Rename(ctx context.Context, oldParent fuseops.InodeID, oldName string, newParent fuseops.InodeID, newName string) error

	LookUp(ctx context.Context, parent fuseops.InodeID, name string) (*Entry, error)
	Get(ctx context.Context, inode fuseops.InodeID) (*Inode, error)
	Touch(ctx context.Context, inode fuseops.InodeID, size *uint64, mode *os.FileMode, atime *time.Time, mtime *time.Time) (*Inode, error)

	AddChunk(ctx context.Context, inode fuseops.InodeID, chunk Chunk) error
	Chunks(ctx context.Context, inode fuseops.InodeID, offset uint64) (ChunkCursor, error)
	Children(ctx context.Context, inode fuseops.InodeID, offset uint64) (ChildCursor, error)

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
	Mode    os.FileMode
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

	// Storage contains the name of the storage service to be used, e.g "wasabi"
	Storage string

	// Credentials contains an identificative name of the credentials to be used
	// when establishing a session with the storage service
	Credentials string

	// Location contains primary location information whithin the storage
	// service, i.e the region in most cases
	Location string

	// Bucket contains secondary location information whithin the storage
	// service, i.e the bucket in most cases
	Bucket string

	// Key represents the name of this binary chunk whithin the provided bucket
	Key string

	// ObjectOffset represents the byte of the object at the storage to take as the
	// first byte of this chunk
	ObjectOffset uint64

	// InodeOffset represents the first byte of the inode that the first byte of
	// this chunk points to
	InodeOffset uint64

	// Size represents the size of this chunk
	Size uint64
}

// ChunkCursor iterates over a certain list of chunks
type ChunkCursor interface {
	Next() (*Chunk, error)
	Close() error
}

// Child represents a child entry within a directory
type Child struct {
	Inode fuseops.InodeID
	Name  string
	Mode  os.FileMode
}

// ChildCursor iterates over a certain list of children
type ChildCursor interface {
	Next() (*Child, error)
	Close() error
}

// ChunkEraser handles object deletion from the respective storage
type ChunkEraser struct {
	erasers map[string]func(chunk Chunk) error
}

// NewEraser initialises a ChunkEraser
func NewEraser() ChunkEraser {
	return ChunkEraser{
		erasers: make(map[string]func(chunk Chunk) error),
	}
}

var errNotSup = errors.New("Storage type not registered in this eraser")

// Erase invokes the eraser function for the corresponding storage
func (ce ChunkEraser) Erase(c Chunk) error {

	eraser, ok := ce.erasers[c.Storage]

	if !ok {
		return errNotSup
	}

	return eraser(c)
}

// SetEraser sets the eraser for a certain storage service
func (ce ChunkEraser) SetEraser(storage string, eraser func(chunk Chunk) error) {
	ce.erasers[storage] = eraser
}
