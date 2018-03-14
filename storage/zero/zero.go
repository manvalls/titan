package zero

import (
	"io"

	"git.vlrz.es/manvalls/titan/storage"
)

// Zero is a /dev/null implementation of the storage interface
type Zero struct {
	Storage string
}

// Setup sets up the storage
func (z *Zero) Setup() error {
	return nil
}

// GetChunk stores the contents of a reader and returns the built chunk
func (z *Zero) GetChunk(reader io.Reader) (*storage.Chunk, error) {
	var err error

	r := &storage.ReaderWithSize{Reader: reader}
	buf := make([]byte, 1e3)
	err = nil

	for err == nil {
		_, err = r.Read(buf)
	}

	filename, err := storage.Key()
	if err != nil {
		return nil, err
	}

	return &storage.Chunk{
		Storage:      z.Storage,
		Key:          filename,
		ObjectOffset: 0,
		Size:         r.Size,
	}, err
}

type zeroReadCloser struct {
	remainingBytes uint64
}

func (zrc *zeroReadCloser) Read(buffer []byte) (n int, err error) {

	for i := 0; i < len(buffer) && zrc.remainingBytes > 0; i++ {
		buffer[i] = 0
		zrc.remainingBytes--
		n++
	}

	if zrc.remainingBytes == 0 {
		err = io.EOF
	}

	return n, err
}

func (zrc *zeroReadCloser) Close() error {
	zrc.remainingBytes = 0
	return nil
}

// GetReadCloser retrieves the contents of a chunk
func (z *Zero) GetReadCloser(chunk storage.Chunk) (io.ReadCloser, error) {
	return &zeroReadCloser{remainingBytes: chunk.Size}, nil
}

// Remove removes a chunk from the storage
func (z *Zero) Remove(chunk storage.Chunk) error {
	return nil
}
