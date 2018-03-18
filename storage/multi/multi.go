package multi

import (
	"errors"
	"io"

	"git.vlrz.es/manvalls/titan/storage"
)

// Multi is an implementation of the storage interface which relies on multiple
// underlying storages
type Multi struct {
	Storages map[string]storage.Storage
	Default  string
}

var errNotSup = errors.New("Storage not supported")

func (m *Multi) getStorage(storage string) (storage.Storage, error) {
	st, ok := m.Storages[storage]
	if !ok {
		return nil, errNotSup
	}

	return st, nil
}

// Setup sets up the storage
func (m *Multi) Setup() error {
	return m.Storages[m.Default].Setup()
}

// GetChunk stores the contents of a reader and returns the built chunk
func (m *Multi) GetChunk(reader io.Reader) (*storage.Chunk, error) {
	st, err := m.getStorage(m.Default)
	if err != nil {
		return nil, err
	}

	return st.GetChunk(reader)
}

// GetReadCloser retrieves the contents of a chunk
func (m *Multi) GetReadCloser(chunk storage.Chunk) (io.ReadCloser, error) {
	st, err := m.getStorage(chunk.Storage)
	if err != nil {
		return nil, err
	}

	return st.GetReadCloser(chunk)
}

// Remove removes a chunk from the storage
func (m *Multi) Remove(chunk storage.Chunk) error {
	st, err := m.getStorage(chunk.Storage)
	if err != nil {
		return err
	}

	return st.Remove(chunk)
}
