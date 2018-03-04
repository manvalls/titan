package writer

import (
	"context"
	"io"
	"sync"

	"git.vlrz.es/manvalls/titan/database"
	"git.vlrz.es/manvalls/titan/storage"
	"github.com/manvalls/fuse/fuseops"
)

// Writer wraps writes to an open file descriptor
type Writer struct {
	database.Db
	storage.Storage
	fuseops.InodeID
	MaxChunkSize int64

	writer io.WriteCloser
	offset int64
	size   int64
	mutex  sync.Mutex
}

// NewWriter builds a writer
func NewWriter() *Writer {
	return &Writer{
		MaxChunkSize: 100e6,
		mutex:        sync.Mutex{},
	}
}

// Flush closes the current open writer, if any
func (w *Writer) Flush() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.flush()
}

func (w *Writer) flush() {
	if w.writer != nil {
		w.writer.Close()
		w.writer = nil
		w.offset = 0
		w.size = 0
	}
}

// WriteAt writes at the specified offset to the current inode
func (w *Writer) WriteAt(p []byte, off int64) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if off != w.offset || w.size+off > w.MaxChunkSize {
		w.flush()
	}

	if w.writer == nil {
		reader, writer := io.Pipe()

		go func() {
			chunk, gcErr := w.GetChunk(reader)
			if gcErr != nil {
				reader.CloseWithError(gcErr)
				return
			}

			w.AddChunk(context.Background(), w.InodeID, database.Chunk{
				Inode:       w.InodeID,
				InodeOffset: uint64(off),
				Chunk:       chunk,
			})
		}()

		w.writer = writer
		w.offset = off
		w.size = 0
	}

	n, err = w.writer.Write(p)
	w.offset += int64(n)

	if err != nil {
		w.flush()
	}

	return n, err
}
