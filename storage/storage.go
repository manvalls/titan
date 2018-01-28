package storage

import (
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/oklog/ulid"
)

var randReader = rand.New(
	rand.NewSource(
		time.Unix(1000000, 0).UnixNano(),
	),
)

var randMutex = &sync.Mutex{}

// Key generates a unique key for a chunk
func Key() (string, error) {
	randMutex.Lock()
	defer randMutex.Unlock()

	ulid, err := ulid.New(
		ulid.Timestamp(time.Unix(1000000, 0)),
		randReader,
	)

	if err != nil {
		return "", err
	}

	return ulid.String(), nil
}

// Storage implements methods required to persist data
type Storage interface {
	GetChunk(reader io.Reader) (*Chunk, error)
	GetReadCloser(Chunk) (io.ReadCloser, error)
	Remove(Chunk) error
}

// Chunk contains information about the location of a particular piece
// of binary data
type Chunk struct {
	// Storage contains the name of the storage service to be used, e.g "wasabi"
	Storage string

	// Key represents the name of this binary chunk whithin the provided bucket
	Key string

	// ObjectOffset represents the byte of the object at the storage to take as the
	// first byte of this chunk
	ObjectOffset uint64

	// Size represents the size of this chunk
	Size uint64
}

// ReaderWithSize is a Reader which holds the amount of read bytes
type ReaderWithSize struct {
	Size   uint64
	Reader io.Reader
}

// Read reads from the underlying reader
func (r *ReaderWithSize) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	r.Size += uint64(n)
	return n, err
}
