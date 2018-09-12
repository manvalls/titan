package writer

import (
	"context"
	"errors"
	"io"
	"sync"
	"syscall"

	"github.com/manvalls/fuse/fuseops"
	"github.com/manvalls/titan/database"
	"github.com/manvalls/titan/storage"
)

const maxInt = 9223372036854775807

var errIsClosed = errors.New("Writer already closed")

// Writer wraps writes to an open file descriptor
type Writer struct {
	database.Db
	storage.Storage
	fuseops.InodeID
	MaxChunkSize int64
	Flags        uint32

	flushError chan error
	writer     io.WriteCloser
	offset     int64
	size       int64
	mutex      *sync.Mutex
	closed     bool
}

// NewWriter builds a writer
func NewWriter() *Writer {
	m := sync.Mutex{}
	return &Writer{
		flushError:   make(chan error),
		MaxChunkSize: 134217728,
		mutex:        &m,
		closed:       false,
	}
}

// Flush closes the current open writer, if any
func (w *Writer) Flush() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.flush()
}

func (w *Writer) flush() error {
	if w.writer != nil {
		w.writer.Close()

		w.writer = nil
		w.size = 0

		return <-w.flushError
	}

	return nil
}

// Close closes the current open writer, if any, and marks the writer as closed
func (w *Writer) Close() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.closed = true
	w.flush()
}

// WriteAt writes at the specified offset to the current inode
func (w *Writer) WriteAt(p []byte, off int64) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return 0, errIsClosed
	}

	if w.Flags&syscall.O_APPEND != 0 {
		off = w.offset
	}

	if off != w.offset || w.size+int64(len(p)) > w.MaxChunkSize {
		w.flush()
	}

	if w.writer == nil {
		reader, writer := io.Pipe()

		go func() {
			chunk, gcErr := w.GetChunk(reader)
			if gcErr != nil {
				reader.CloseWithError(gcErr)
				w.flushError <- gcErr
				return
			}

			w.flushError <- w.AddChunk(context.Background(), w.InodeID, w.Flags, database.Chunk{
				Inode:       w.InodeID,
				InodeOffset: uint64(off),
				Chunk:       *chunk,
			})
		}()

		w.writer = writer
		w.offset = off
		w.size = 0
	}

	n, err = w.writer.Write(p)
	w.offset += int64(n)
	w.size += int64(n)

	if err != nil {
		w.flush()
	}

	return n, err
}
