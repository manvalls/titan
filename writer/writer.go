package writer

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/manvalls/titan/database"
	"github.com/manvalls/titan/math"
	"github.com/manvalls/titan/storage"
	"github.com/manvalls/fuse/fuseops"
)

const maxInt = 9223372036854775807

var errIsClosed = errors.New("Writer already closed")

// Writer wraps writes to an open file descriptor
type Writer struct {
	database.Db
	storage.Storage
	fuseops.InodeID
	MaxChunkSize int64
	WaitTimeout  time.Duration

	ts                time.Time
	flushError        chan error
	writer            io.WriteCloser
	offset            int64
	size              int64
	mutex             *sync.Mutex
	cond              *sync.Cond
	sleeping          bool
	closed            bool
	minAwaitingOffset int64
}

// NewWriter builds a writer
func NewWriter() *Writer {
	m := sync.Mutex{}
	return &Writer{
		ts:                time.Now(),
		flushError:        make(chan error),
		MaxChunkSize:      134217728,
		WaitTimeout:       1 * time.Second,
		mutex:             &m,
		cond:              sync.NewCond(&m),
		sleeping:          false,
		closed:            false,
		minAwaitingOffset: maxInt,
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

		err := <-w.flushError
		w.ts = time.Now()
		return err
	}

	return nil
}

// Close closes the current open writer, if any, and marks the writer as closed
func (w *Writer) Close() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	defer w.cond.Broadcast()
	w.closed = true
	w.flush()
}

// WriteAt writes at the specified offset to the current inode
func (w *Writer) WriteAt(p []byte, off int64) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	defer w.cond.Broadcast()

	for {
		w.minAwaitingOffset = math.MinInt(w.minAwaitingOffset, off)

		if w.closed {
			return 0, errIsClosed
		}

		if w.size+int64(len(p)) > w.MaxChunkSize {
			w.flush()
		}

		if off == w.offset {
			break
		}

		inactivity := time.Now().Sub(w.ts)
		if w.minAwaitingOffset == off && (off < w.offset || inactivity > w.WaitTimeout) {
			w.flush()
			break
		}

		if !w.sleeping {
			w.sleeping = true
			go func() {
				time.Sleep(w.WaitTimeout - inactivity)

				w.mutex.Lock()
				defer w.mutex.Unlock()
				defer w.cond.Broadcast()
				w.sleeping = false
			}()
		}

		w.cond.Wait()
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

			w.flushError <- w.AddChunk(context.Background(), w.InodeID, database.Chunk{
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

	w.ts = time.Now()
	w.minAwaitingOffset = maxInt
	return n, err
}
