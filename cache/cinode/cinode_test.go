package cinode

import (
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fujiwara/shapeio"
	"github.com/manvalls/titan/database"
	"github.com/manvalls/titan/storage"
	"github.com/stretchr/testify/assert"
)

type testStorage struct{}

func (t testStorage) Setup() error {
	return nil
}

func (t testStorage) GetChunk(reader io.Reader) (*storage.Chunk, error) {
	return nil, nil
}

func (t testStorage) GetReadCloser(chunk storage.Chunk) (io.ReadCloser, error) {
	testString := ""
	for uint64(len(testString)) < chunk.Size {
		testString += chunk.Key
	}

	testString = testString[:chunk.Size]
	reader := shapeio.NewReader(strings.NewReader(testString))
	reader.SetRateLimit(10)
	return ioutil.NopCloser(reader), nil
}

func (t testStorage) Remove(storage.Chunk) error {
	return nil
}

func getTestInode() *Inode {
	inode := NewInode()
	inode.Storage = testStorage{}
	inode.BufferSize = 2

	tmpKey, _ := storage.Key()
	inode.Path = "/tmp/" + tmpKey
	return inode
}

type inodeReader struct {
	offset int
	sync.Mutex
	*Inode
}

func (ir *inodeReader) Read(b []byte) (n int, err error) {
	ir.Lock()
	defer ir.Unlock()

	n, err = ir.ReadAt(b, int64(ir.offset))
	ir.offset += n
	return
}

func getReader(i *Inode) *inodeReader {
	return &inodeReader{Inode: i}
}

func TestSingleReadAll(t *testing.T) {
	inode := getTestInode()
	inode.Size = 5
	inode.Chunks = []database.Chunk{
		{Chunk: storage.Chunk{Key: "a", Size: 2}},
		{Chunk: storage.Chunk{Key: "b", Size: 3}, InodeOffset: 2},
	}

	result, _ := ioutil.ReadAll(getReader(inode))
	assert.Equal(t, string(result), "aabbb")
}

func TestSingleReadAllWrongOffset(t *testing.T) {
	inode := getTestInode()
	inode.Size = 5
	inode.Chunks = []database.Chunk{
		{Chunk: storage.Chunk{Key: "a", Size: 2}},
		{Chunk: storage.Chunk{Key: "b", Size: 3}},
	}

	result, _ := ioutil.ReadAll(getReader(inode))
	assert.Equal(t, string(result), "aab")
}

func TestMultipleReadAll(t *testing.T) {
	inode := getTestInode()
	inode.Size = 5
	inode.Chunks = []database.Chunk{
		{Chunk: storage.Chunk{Key: "a", Size: 2}},
		{Chunk: storage.Chunk{Key: "b", Size: 3}, InodeOffset: 2},
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			result, _ := ioutil.ReadAll(getReader(inode))
			assert.Equal(t, string(result), "aabbb")
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestReadAtAfterSleep(t *testing.T) {
	inode := getTestInode()
	inode.Size = 20
	inode.Chunks = []database.Chunk{
		{Chunk: storage.Chunk{Key: "a", Size: 10}},
		{Chunk: storage.Chunk{Key: "b", Size: 10}, InodeOffset: 10},
	}

	b := make([]byte, 1)
	inode.ReadAt(b, 0)
	assert.Equal(t, string(b), "a")

	time.Sleep(200 * time.Millisecond)
	inode.ReadAt(b, 19)
	assert.Equal(t, string(b), "b")
}

func TestReadAtAfterSleepLowThreshold(t *testing.T) {
	inode := getTestInode()
	inode.Size = 20
	inode.MaxOffsetDistance = 1
	inode.InactivityTimeout = 100 * time.Millisecond
	inode.Chunks = []database.Chunk{
		{Chunk: storage.Chunk{Key: "a", Size: 10}},
		{Chunk: storage.Chunk{Key: "b", Size: 10}, InodeOffset: 10},
	}

	b := make([]byte, 1)
	inode.ReadAt(b, 0)
	assert.Equal(t, string(b), "a")

	time.Sleep(200 * time.Millisecond)
	inode.ReadAt(b, 19)
	assert.Equal(t, string(b), "b")
	inode.ReadAt(b, 0)
	assert.Equal(t, string(b), "a")
	inode.ReadAt(b, 1)
	assert.Equal(t, string(b), "a")
	inode.ReadAt(b, 2)
	assert.Equal(t, string(b), "a")
	inode.ReadAt(b, 18)
	assert.Equal(t, string(b), "b")
}

func TestReadAtAfterSleepShort(t *testing.T) {
	inode := getTestInode()
	inode.Size = 20
	inode.Chunks = []database.Chunk{
		{Chunk: storage.Chunk{Key: "a", Size: 10}},
		{Chunk: storage.Chunk{Key: "b", Size: 10}, InodeOffset: 10},
	}

	b := make([]byte, 1)
	inode.ReadAt(b, 0)
	assert.Equal(t, string(b), "a")

	time.Sleep(200 * time.Millisecond)
	inode.ReadAt(b, 1)
	assert.Equal(t, string(b), "a")
}

func TestReadAllAfterSleep(t *testing.T) {
	inode := getTestInode()
	inode.Size = 20
	inode.Chunks = []database.Chunk{
		{Chunk: storage.Chunk{Key: "a", Size: 10}},
		{Chunk: storage.Chunk{Key: "b", Size: 10}, InodeOffset: 10},
	}

	b := make([]byte, 1)
	inode.ReadAt(b, 11)
	assert.Equal(t, string(b), "b")

	time.Sleep(200 * time.Millisecond)
	result, _ := ioutil.ReadAll(getReader(inode))
	assert.Equal(t, string(result), "aaaaaaaaaabbbbbbbbbb")
}
