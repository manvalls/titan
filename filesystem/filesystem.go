package filesystem

import (
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"git.vlrz.es/manvalls/titan/cache"
	"git.vlrz.es/manvalls/titan/database"
	"git.vlrz.es/manvalls/titan/storage"
	"git.vlrz.es/manvalls/titan/writer"
	"github.com/manvalls/fuse/fuseops"
	"github.com/manvalls/fuse/fuseutil"
)

const (
	totalBlocks = 281474976710656
	totalInodes = 281474976710656
	blockSize   = 4096
	ioSize      = 65536
)

// FileSystem implements the fuse filesystem interface
type FileSystem struct {
	storage.Storage
	database.Db
	*cache.Cache

	AttributesExpiration time.Duration
	EntryExpiration      time.Duration
	MaxChunkSize         int64

	nextHandle fuseops.HandleID

	writers map[fuseops.HandleID]*writer.Writer
	folders map[fuseops.HandleID]*[]database.Child
	lookups map[fuseops.InodeID]uint64

	writerMutex sync.Mutex
	folderMutex sync.Mutex
	lookupMutex sync.Mutex
	handleMutex sync.Mutex
}

// NewFileSystem constructs a FileSystem
func NewFileSystem() *FileSystem {
	return &FileSystem{
		writers: make(map[fuseops.HandleID]*writer.Writer),
		folders: make(map[fuseops.HandleID]*[]database.Child),
		lookups: make(map[fuseops.InodeID]uint64),

		AttributesExpiration: 10 * time.Second,
		EntryExpiration:      10 * time.Second,
		MaxChunkSize:         100e6,

		writerMutex: sync.Mutex{},
		lookupMutex: sync.Mutex{},
		folderMutex: sync.Mutex{},
		handleMutex: sync.Mutex{},
	}
}

func (fs *FileSystem) handle() fuseops.HandleID {
	fs.handleMutex.Lock()
	defer fs.handleMutex.Unlock()
	handle := fs.nextHandle
	fs.nextHandle++
	return handle
}

func (fs *FileSystem) incLookup(inode fuseops.InodeID) {
	fs.lookupMutex.Lock()
	defer fs.lookupMutex.Unlock()
	fs.lookups[inode]++
}

func (fs *FileSystem) fillChildEntry(entry *fuseops.ChildInodeEntry, inode database.Inode) {
	entry.Child = inode.ID
	entry.Attributes = inode.InodeAttributes
	entry.AttributesExpiration = time.Now().Add(fs.AttributesExpiration)
	entry.EntryExpiration = time.Now().Add(fs.EntryExpiration)
}

func (fs *FileSystem) children(ctx context.Context, op *fuseops.ReadDirOp) (*[]database.Child, error) {
	fs.folderMutex.Lock()
	defer fs.folderMutex.Unlock()

	children, ok := fs.folders[op.Handle]
	if ok {
		return children, nil
	}

	children, err := fs.Db.Children(ctx, op.Inode)
	if err != nil {
		return nil, err
	}

	fs.folders[op.Handle] = children
	return children, nil
}

func (fs *FileSystem) writer(ctx context.Context, handle fuseops.HandleID, inode fuseops.InodeID) *writer.Writer {
	fs.writerMutex.Lock()
	defer fs.writerMutex.Unlock()

	w, ok := fs.writers[handle]
	if ok {
		return w
	}

	w = writer.NewWriter()
	w.Db = fs.Db
	w.Storage = fs.Storage
	w.InodeID = inode
	w.MaxChunkSize = fs.MaxChunkSize

	fs.writers[handle] = w
	return w
}

// StatFS provides some information about the FS usage
func (fs *FileSystem) StatFS(ctx context.Context, op *fuseops.StatFSOp) error {
	stats, err := fs.Stats(ctx)
	if err != nil {
		return err
	}

	op.BlockSize = blockSize
	op.IoSize = ioSize
	op.Blocks = totalBlocks
	op.Inodes = totalInodes

	op.InodesFree = totalInodes - stats.Inodes
	op.BlocksFree = totalBlocks - (stats.Size / blockSize)
	op.BlocksAvailable = op.BlocksFree
	return nil
}

// LookUpInode fills some information about the requested entry
func (fs *FileSystem) LookUpInode(ctx context.Context, op *fuseops.LookUpInodeOp) error {
	entry, err := fs.LookUp(ctx, op.Parent, op.Name)
	if err != nil {
		return err
	}

	fs.fillChildEntry(&op.Entry, entry.Inode)
	fs.incLookup(entry.Inode.ID)
	return nil
}

// GetInodeAttributes gets the attributes of an inode
func (fs *FileSystem) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) error {
	inode, err := fs.Get(ctx, op.Inode)
	if err != nil {
		return err
	}

	op.Attributes = inode.InodeAttributes
	op.AttributesExpiration = time.Now().Add(fs.EntryExpiration)
	return nil
}

// SetInodeAttributes sets the attributes of an inode
func (fs *FileSystem) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) error {
	inode, err := fs.Touch(ctx, op.Inode, op.Size, op.Mode, op.Atime, op.Mtime, op.Uid, op.Gid)
	if err != nil {
		return err
	}

	op.Attributes = inode.InodeAttributes
	op.AttributesExpiration = time.Now().Add(fs.EntryExpiration)
	return nil
}

// ForgetInode decrements the lookup count for the given inode
func (fs *FileSystem) ForgetInode(ctx context.Context, op *fuseops.ForgetInodeOp) error {
	fs.lookupMutex.Lock()
	defer fs.lookupMutex.Unlock()

	lookups := fs.lookups[op.Inode]
	if lookups <= op.N {
		delete(fs.lookups, op.Inode)
		return fs.Forget(ctx, op.Inode)
	}

	fs.lookups[op.Inode] -= op.N
	return nil
}

// MkDir creates a new directory
func (fs *FileSystem) MkDir(ctx context.Context, op *fuseops.MkDirOp) error {
	entry, err := fs.Create(ctx, database.Entry{
		Parent: op.Parent,
		Name:   op.Name,
		Inode: database.Inode{
			InodeAttributes: fuseops.InodeAttributes{
				Mode: op.Mode | os.ModeDir,
				Uid:  op.Uid,
				Gid:  op.Gid,
			},
		},
	})

	if err != nil {
		return err
	}

	fs.fillChildEntry(&op.Entry, entry.Inode)
	fs.incLookup(entry.Inode.ID)
	return nil
}

// MkNode creates a new node
func (fs *FileSystem) MkNode(ctx context.Context, op *fuseops.MkNodeOp) error {
	entry, err := fs.Create(ctx, database.Entry{
		Parent: op.Parent,
		Name:   op.Name,
		Inode: database.Inode{
			InodeAttributes: fuseops.InodeAttributes{
				Mode: op.Mode,
				Uid:  op.Uid,
				Gid:  op.Gid,
			},
		},
	})

	if err != nil {
		return err
	}

	fs.fillChildEntry(&op.Entry, entry.Inode)
	fs.incLookup(entry.Inode.ID)
	return nil
}

// CreateFile creates a new file
func (fs *FileSystem) CreateFile(ctx context.Context, op *fuseops.CreateFileOp) error {
	entry, err := fs.Create(ctx, database.Entry{
		Parent: op.Parent,
		Name:   op.Name,
		Inode: database.Inode{
			InodeAttributes: fuseops.InodeAttributes{
				Mode: op.Mode,
				Uid:  op.Uid,
				Gid:  op.Gid,
			},
		},
	})

	if err != nil {
		return err
	}

	op.Handle = fs.handle()
	fs.fillChildEntry(&op.Entry, entry.Inode)
	fs.incLookup(entry.Inode.ID)
	return nil
}

// CreateLink creates a new hard link
func (fs *FileSystem) CreateLink(ctx context.Context, op *fuseops.CreateLinkOp) error {
	entry, err := fs.Create(ctx, database.Entry{
		Parent: op.Parent,
		Name:   op.Name,
		Inode: database.Inode{
			ID: op.Target,
		},
	})

	if err != nil {
		return err
	}

	fs.fillChildEntry(&op.Entry, entry.Inode)
	fs.incLookup(entry.Inode.ID)
	return nil
}

// CreateSymlink creates a new symbolic link
func (fs *FileSystem) CreateSymlink(ctx context.Context, op *fuseops.CreateSymlinkOp) error {
	entry, err := fs.Create(ctx, database.Entry{
		Parent: op.Parent,
		Name:   op.Name,
		Inode: database.Inode{
			SymLink: op.Target,
			InodeAttributes: fuseops.InodeAttributes{
				Mode: os.ModeSymlink | 0777,
				Uid:  op.Uid,
				Gid:  op.Gid,
			},
		},
	})

	if err != nil {
		return err
	}

	fs.fillChildEntry(&op.Entry, entry.Inode)
	fs.incLookup(entry.Inode.ID)
	return nil
}

// Rename renames an entry
func (fs *FileSystem) Rename(ctx context.Context, op *fuseops.RenameOp) error {
	return fs.Db.Rename(ctx, op.OldParent, op.OldName, op.NewParent, op.NewName)
}

// RmDir removes a directory from the filesystem
func (fs *FileSystem) RmDir(ctx context.Context, op *fuseops.RmDirOp) error {
	return fs.Db.Unlink(ctx, op.Parent, op.Name, true)
}

// Unlink removes an entry from the filesystem
func (fs *FileSystem) Unlink(ctx context.Context, op *fuseops.UnlinkOp) error {
	return fs.Db.Unlink(ctx, op.Parent, op.Name, false)
}

// OpenDir generates a handle for the given dir
func (fs *FileSystem) OpenDir(ctx context.Context, op *fuseops.OpenDirOp) error {
	inode, err := fs.Get(ctx, op.Inode)
	if err != nil {
		return err
	}

	if !inode.Mode.IsDir() {
		return syscall.ENOTDIR
	}

	op.Handle = fs.handle()
	return nil
}

// ReadDir reads several entries from the given directory
func (fs *FileSystem) ReadDir(ctx context.Context, op *fuseops.ReadDirOp) error {
	children, err := fs.children(ctx, op)
	if err != nil {
		return err
	}

	n := 0

	for i, length := uint64(op.Offset), uint64(len(*children)); i < length; i++ {
		child := (*children)[i]
		d := fuseutil.Dirent{
			Offset: fuseops.DirOffset(i + 1),
			Inode:  child.Inode,
			Name:   child.Name,
		}

		switch {
		case child.Mode&os.ModeDir != 0:
			d.Type = fuseutil.DT_Directory
		case child.Mode&os.ModeSocket != 0:
			d.Type = fuseutil.DT_Socket
		case child.Mode&os.ModeSymlink != 0:
			d.Type = fuseutil.DT_Link
		case child.Mode&os.ModeNamedPipe != 0:
			d.Type = fuseutil.DT_FIFO
		case child.Mode&os.ModeCharDevice != 0:
			d.Type = fuseutil.DT_Char
		case child.Mode&os.ModeDevice != 0:
			d.Type = fuseutil.DT_Block
		default:
			d.Type = fuseutil.DT_File
		}

		written := fuseutil.WriteDirent(op.Dst[n:], d)
		n += written

		if written == 0 {
			break
		}
	}

	op.BytesRead = n
	return nil
}

// ReleaseDirHandle removes the resources associated with a directory handle
func (fs *FileSystem) ReleaseDirHandle(ctx context.Context, op *fuseops.ReleaseDirHandleOp) error {
	fs.folderMutex.Lock()
	defer fs.folderMutex.Unlock()
	delete(fs.folders, op.Handle)
	return nil
}

// OpenFile generates a handle for the given file
func (fs *FileSystem) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) error {
	inode, err := fs.Get(ctx, op.Inode)
	if err != nil {
		return err
	}

	if inode.Mode.IsDir() {
		return syscall.EISDIR
	}

	op.Handle = fs.handle()
	fs.Validate(op.Inode)
	return nil
}

// ReadFile reads the contents of a file from the cache
func (fs *FileSystem) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) error {
	n := 0

	for length := len(op.Dst); n < length; {
		bytesRead, err := fs.ReadInodeAt(op.Inode, op.Dst[n:], op.Offset+int64(n))
		n += bytesRead

		if err != nil {
			if err != io.EOF {
				return err
			}

			break
		}
	}

	op.BytesRead = n
	return nil
}

// WriteFile writes content to a file
func (fs *FileSystem) WriteFile(ctx context.Context, op *fuseops.WriteFileOp) error {
	w := fs.writer(ctx, op.Handle, op.Inode)
	_, err := w.WriteAt(op.Data, op.Offset)
	return err
}

// SyncFile flushes a writer
func (fs *FileSystem) SyncFile(ctx context.Context, op *fuseops.SyncFileOp) error {
	w := fs.writer(ctx, op.Handle, op.Inode)
	err := w.Flush()
	fs.Validate(op.Inode)
	return err
}

// FlushFile flushes a writer
func (fs *FileSystem) FlushFile(ctx context.Context, op *fuseops.FlushFileOp) error {
	w := fs.writer(ctx, op.Handle, op.Inode)
	err := w.Flush()
	fs.Validate(op.Inode)
	return err
}

// ReleaseFileHandle cleans the resources of a handle
func (fs *FileSystem) ReleaseFileHandle(ctx context.Context, op *fuseops.ReleaseFileHandleOp) error {
	fs.writerMutex.Lock()
	defer fs.writerMutex.Unlock()
	delete(fs.writers, op.Handle)
	return nil
}

// ReadSymlink reads a symbolic link
func (fs *FileSystem) ReadSymlink(ctx context.Context, op *fuseops.ReadSymlinkOp) error {
	inode, err := fs.Get(ctx, op.Inode)
	if err != nil {
		return err
	}

	op.Target = inode.SymLink
	return nil
}

// RemoveXattr removes an extended attribute
func (fs *FileSystem) RemoveXattr(ctx context.Context, op *fuseops.RemoveXattrOp) error {
	return fs.Db.RemoveXattr(ctx, op.Inode, op.Name)
}

// GetXattr retrieves the value of an extended attribute
func (fs *FileSystem) GetXattr(ctx context.Context, op *fuseops.GetXattrOp) error {
	value, err := fs.Db.GetXattr(ctx, op.Inode, op.Name)
	if err != nil {
		return err
	}

	op.BytesRead = len(*value)
	if op.BytesRead > len(op.Dst) {
		return syscall.ERANGE
	}

	copy(op.Dst, *value)
	return nil
}

// ListXattr lists the extended attributes of an inode
func (fs *FileSystem) ListXattr(ctx context.Context, op *fuseops.ListXattrOp) error {
	attrs, err := fs.Db.ListXattr(ctx, op.Inode)
	if err != nil {
		return err
	}

	result := []byte(strings.Join(*attrs, "\000"))
	result = append(result, 0)

	op.BytesRead = len(result)
	if op.BytesRead > len(op.Dst) {
		return syscall.ERANGE
	}

	copy(op.Dst, result)
	return nil
}

// SetXattr sets an extended attribute at the given inode
func (fs *FileSystem) SetXattr(ctx context.Context, op *fuseops.SetXattrOp) error {
	return fs.Db.SetXattr(ctx, op.Inode, op.Name, op.Value, op.Flags)
}

// Destroy frees the resources associated with this file system
func (fs *FileSystem) Destroy() {
	fs.Db.CleanOrphanInodes(context.Background())
	fs.Cache.Destroy()
}
