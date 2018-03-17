package mysql

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"git.vlrz.es/manvalls/titan/database"
	"git.vlrz.es/manvalls/titan/math"
	"git.vlrz.es/manvalls/titan/storage"
	"github.com/manvalls/fuse/fuseops"

	// mysql driver for the sql package
	_ "github.com/go-sql-driver/mysql"
)

// Driver implements the Db interface for the titan file system
type Driver struct {
	DbURI string
	*sql.DB
}

// Open opens the underlying connection
func (d *Driver) Open() error {
	db, err := sql.Open("mysql", d.DbURI+"?parseTime=true")
	if err != nil {
		return err
	}

	d.DB = db
	return nil
}

// Close closes the underlying connection
func (d *Driver) Close() error {
	return d.DB.Close()
}

// Setup creates the tables and the initial data required by the file system
func (d *Driver) Setup(ctx context.Context) error {
	tx, err := d.DB.BeginTx(ctx, nil)

	if err != nil {
		return err
	}

	queries := []string{
		"CREATE TABLE inodes ( id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, mode INT UNSIGNED NOT NULL, gid INT UNSIGNED NOT NULL, uid INT UNSIGNED NOT NULL, target VARBINARY(4096) NOT NULL DEFAULT \"\", size BIGINT UNSIGNED NOT NULL, refcount INT UNSIGNED NOT NULL, atime DATETIME NOT NULL, mtime DATETIME NOT NULL, ctime DATETIME NOT NULL, crtime DATETIME NOT NULL, PRIMARY KEY (id) )",

		"CREATE TABLE entries (parent BIGINT UNSIGNED NOT NULL, name VARBINARY(255) NOT NULL, inode BIGINT UNSIGNED NOT NULL, PRIMARY KEY (parent, name), INDEX (parent), INDEX (inode), FOREIGN KEY (parent) REFERENCES inodes(id), FOREIGN KEY (inode) REFERENCES inodes(id))",

		"CREATE TABLE chunks (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, inode BIGINT UNSIGNED, storage VARCHAR(255), `key` VARCHAR(255), objectoffset BIGINT, inodeoffset BIGINT, size BIGINT, orphandate DATETIME, PRIMARY KEY (id), INDEX (inode), FOREIGN KEY (inode) REFERENCES inodes(id))",

		"CREATE TABLE xattr (inode BIGINT UNSIGNED NOT NULL, `key` VARBINARY(255) NOT NULL, value VARBINARY(4096) NOT NULL, PRIMARY KEY (inode, `key`), INDEX (inode), FOREIGN KEY (inode) REFERENCES inodes(id))",

		"CREATE TABLE stats (inodes BIGINT UNSIGNED NOT NULL, size BIGINT UNSIGNED NOT NULL)",

		"INSERT INTO inodes(id, mode, uid, gid, size, refcount, atime, mtime, ctime, crtime) VALUES(1, 2147484159, 0, 0, 4096, 2, NOW(), NOW(), NOW(), NOW())",
		"INSERT INTO entries(parent, name, inode) VALUES(1, '.', 1)",
		"INSERT INTO entries(parent, name, inode) VALUES(1, '..', 1)",
		"INSERT INTO stats(inodes, size) VALUES(1, 4096)",
	}

	for _, query := range queries {
		_, err = tx.Exec(query)

		if err != nil {
			tx.Rollback()
			return treatError(err)
		}
	}

	return tx.Commit()
}

// Stats retrieves the file system stats
func (d *Driver) Stats(ctx context.Context) (*database.Stats, error) {
	stats := database.Stats{}
	row := d.DB.QueryRowContext(ctx, "SELECT inodes, size FROM stats")
	err := row.Scan(&stats.Inodes, &stats.Size)

	if err != nil {
		return nil, treatError(err)
	}

	return &stats, nil
}

// Create creates a new inode or link
func (d *Driver) Create(ctx context.Context, entry database.Entry) (*database.Entry, error) {
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, treatError(err)
	}

	parentInode, err := d.getInode(tx, entry.Parent)
	if err != nil {
		tx.Rollback()
		return nil, treatError(err)
	}

	if !parentInode.Mode.IsDir() {
		tx.Rollback()
		return nil, syscall.ENOTDIR
	}

	fillInode := func() error {
		result, ierr := d.getInode(tx, entry.ID)
		if ierr != nil {
			tx.Rollback()
			return treatError(ierr)
		}

		entry.Inode = *result
		return nil
	}

	needsRefcountChange := true

	if entry.ID == 0 {
		var result sql.Result
		var size, refcount uint64
		var id int64

		needsRefcountChange = false

		if entry.Mode.IsDir() {
			refcount = 2
			size = 4096
		} else {
			refcount = 1
			size = 0
		}

		if _, err = tx.Exec("UPDATE stats SET inodes = inodes + 1, size = size + ?", size); err != nil {
			tx.Rollback()
			return nil, treatError(err)
		}

		result, err = tx.Exec("INSERT INTO inodes(mode, uid, gid, size, refcount, atime, mtime, ctime, crtime, target) VALUES(?, ?, ?, ?, ?, NOW(), NOW(), NOW(), NOW(), ?)", uint32(entry.Mode), entry.Uid, entry.Gid, size, refcount, entry.SymLink)
		if err != nil {
			tx.Rollback()
			return nil, treatError(err)
		}

		id, err = result.LastInsertId()
		if err != nil {
			tx.Rollback()
			return nil, treatError(err)
		}

		entry.ID = fuseops.InodeID(id)

		if entry.Mode.IsDir() {

			_, err = tx.Exec("INSERT INTO entries(parent, name, inode) VALUES (?, '.', ?), (?, '..', ?)", uint64(entry.ID), uint64(entry.ID), uint64(entry.ID), uint64(entry.Parent))
			if err != nil {
				tx.Rollback()
				return nil, treatError(err)
			}

			_, err = tx.Exec("UPDATE inodes SET refcount = refcount + 1 WHERE id = ?", uint64(entry.Parent))
			if err != nil {
				tx.Rollback()
				return nil, treatError(err)
			}

		}

		if err = fillInode(); err != nil {
			return nil, err
		}

	} else {

		if err = fillInode(); err != nil {
			return nil, err
		}

		if entry.Mode.IsDir() {
			tx.Rollback()
			return nil, syscall.EISDIR
		}

	}

	_, err = tx.Exec("INSERT INTO entries(parent, name, inode) VALUES(?, ?, ?)", uint64(entry.Parent), []byte(entry.Name), uint64(entry.ID))
	if err != nil {
		tx.Rollback()
		return nil, treatError(err)
	}

	if needsRefcountChange {
		_, err = tx.Exec("UPDATE inodes SET refcount = refcount + 1 WHERE id = ?", uint64(entry.ID))
		if err != nil {
			tx.Rollback()
			return nil, treatError(err)
		}
	}

	return &entry, tx.Commit()
}

// Forget checks if an inode has any links and removes it if not
func (d *Driver) Forget(ctx context.Context, inode fuseops.InodeID) error {
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return treatError(err)
	}

	in, err := d.getInode(tx, inode)
	if err != nil {
		tx.Rollback()
		return treatError(err)
	}

	chunks := make([]string, 0, 1)

	if in.Nlink == 0 {
		var rows *sql.Rows

		rows, err = tx.Query("SELECT id FROM chunks WHERE inode = ?", in.ID)
		if err != nil {
			tx.Rollback()
			return treatError(err)
		}

		defer rows.Close()

		for rows.Next() {
			var id uint64

			err = rows.Scan(
				&id,
			)

			if err != nil {
				tx.Rollback()
				return treatError(err)
			}

			chunks = append(chunks, strconv.FormatUint(id, 10))
		}

		if _, err = tx.Exec("DELETE FROM inodes WHERE id = ?", uint64(in.ID)); err != nil {
			tx.Rollback()
			return treatError(err)
		}

		if _, err = tx.Exec("UPDATE stats SET size = size - ?, inodes = inodes - 1", in.Size); err != nil {
			tx.Rollback()
			return treatError(err)
		}

	}

	if len(chunks) > 0 {
		if _, err = tx.Exec("UPDATE chunks SET inode = NULL, objectoffset = NULL, inodeoffset = NULL, size = NULL, orphandate = NOW() WHERE id IN (" + strings.Join(chunks, ", ") + ")"); err != nil {
			tx.Rollback()
			return treatError(err)
		}
	}

	if err = tx.Commit(); err != nil {
		return treatError(err)
	}

	return nil
}

// CleanOrphanInodes removes all orphan inodes and chunks
func (d *Driver) CleanOrphanInodes(ctx context.Context) error {
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return treatError(err)
	}

	chunks := make([]string, 0, 1)

	rows, err := tx.Query("SELECT c.id, c.size FROM chunks c, inodes i WHERE c.inode = i.id AND i.refcount = 0")

	if err != nil {
		tx.Rollback()
		return treatError(err)
	}

	defer rows.Close()

	var size uint64

	for rows.Next() {
		var id uint64
		var csize uint64

		err = rows.Scan(
			&id,
			&csize,
		)

		if err != nil {
			tx.Rollback()
			return treatError(err)
		}

		size += csize
		chunks = append(chunks, strconv.FormatUint(id, 10))
	}

	result, err := tx.Exec("DELETE FROM inodes WHERE refcount = 0")
	if err != nil {
		tx.Rollback()
		return treatError(err)
	}

	inodes, err := result.RowsAffected()
	if err != nil {
		tx.Rollback()
		return treatError(err)
	}

	if _, err = tx.Exec("UPDATE stats SET size = size - ?, inodes = inodes - ?", size, inodes); err != nil {
		tx.Rollback()
		return treatError(err)
	}

	if len(chunks) > 0 {
		if _, err = tx.Exec("UPDATE chunks SET inode = NULL, objectoffset = NULL, inodeoffset = NULL, size = NULL, orphandate = NOW() WHERE id IN (" + strings.Join(chunks, ", ") + ")"); err != nil {
			tx.Rollback()
			return treatError(err)
		}
	}

	if err = tx.Commit(); err != nil {
		return treatError(err)
	}

	return nil
}

// CleanOrphanChunks removes orphaned chunks
func (d *Driver) CleanOrphanChunks(ctx context.Context, threshold time.Time, st storage.Storage, workers int) error {
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	rows, err := tx.Query("SELECT storage, `key` FROM chunks WHERE inode IS NULL AND orphandate < ?", threshold)
	if err != nil {
		tx.Rollback()
		return err
	}

	ch := make(chan storage.Chunk)

	for i := 0; i < workers; i++ {
		go func() {
			for chunk := range ch {
				st.Remove(chunk)
			}
		}()
	}

	for rows.Next() {
		chunk := storage.Chunk{}

		err = rows.Scan(
			&chunk.Storage,
			&chunk.Key,
		)

		if err != nil {
			close(ch)
			return err
		}

		ch <- chunk
	}

	close(ch)

	_, err = tx.Exec("DELETE FROM chunks WHERE inode IS NULL AND orphandate < ?", threshold)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// Unlink removes an entry from the file system
func (d *Driver) Unlink(ctx context.Context, parent fuseops.InodeID, name string, isFolder bool) error {
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return treatError(err)
	}

	var inode, children uint64

	row := tx.QueryRow("SELECT pe.inode, (SELECT count(*) FROM entries ce WHERE ce.parent = pe.inode) as children FROM entries pe WHERE pe.parent = ? AND pe.name = ?", uint64(parent), name)

	if err = row.Scan(&inode, &children); err != nil {
		tx.Rollback()
		return treatError(err)
	}

	if children > 2 {
		tx.Rollback()
		return syscall.ENOTEMPTY
	}

	var expectedChildren uint64
	var wrongTypeError error

	if isFolder {
		expectedChildren = 2
		wrongTypeError = syscall.ENOTDIR
	} else {
		expectedChildren = 0
		wrongTypeError = syscall.EISDIR
	}

	if children != expectedChildren {
		tx.Rollback()
		return wrongTypeError
	}

	if isFolder {
		if _, err = tx.Exec("DELETE FROM entries WHERE inode = ? OR parent = ?", uint64(inode), uint64(inode)); err != nil {
			tx.Rollback()
			return treatError(err)
		}

		if _, err = tx.Exec("UPDATE inodes SET refcount = refcount - 2 WHERE id = ?", uint64(inode)); err != nil {
			tx.Rollback()
			return treatError(err)
		}

		if _, err = tx.Exec("UPDATE inodes SET refcount = refcount - 1 WHERE id = ?", uint64(parent)); err != nil {
			tx.Rollback()
			return treatError(err)
		}
	} else {
		if _, err = tx.Exec("DELETE FROM entries WHERE parent = ? AND name = ?", uint64(parent), name); err != nil {
			tx.Rollback()
			return treatError(err)
		}

		if _, err = tx.Exec("UPDATE inodes SET refcount = refcount - 1 WHERE id = ?", uint64(inode)); err != nil {
			tx.Rollback()
			return treatError(err)
		}
	}

	if err = tx.Commit(); err != nil {
		return treatError(err)
	}

	return nil
}

// Rename renames an entry
func (d *Driver) Rename(ctx context.Context, oldParent fuseops.InodeID, oldName string, newParent fuseops.InodeID, newName string) error {
	result, err := d.DB.ExecContext(ctx, "UPDATE entries SET parent = ?, name = ? WHERE parent = ?, name = ?", uint64(newParent), newName, uint64(oldParent), oldName)

	if err != nil {
		return treatError(err)
	}

	if rowsAffected, _ := result.RowsAffected(); rowsAffected == 0 {
		return syscall.ENOENT
	}

	return nil
}

// LookUp finds the entry located under the specified parent with the specified name
func (d *Driver) LookUp(ctx context.Context, parent fuseops.InodeID, name string) (*database.Entry, error) {
	row := d.DB.QueryRowContext(ctx, "SELECT i.id, i.mode, i.uid, i.gid, i.size, i.refcount, i.atime, i.mtime, i.ctime, i.crtime, i.target FROM inodes i, entries e WHERE i.id = e.inode AND e.parent = ? AND e.name = ?", uint64(parent), name)

	var mode uint32
	var id uint64
	inode := database.Inode{}

	err := row.Scan(&id, &mode, &inode.Uid, &inode.Gid, &inode.Size, &inode.Nlink, &inode.Atime, &inode.Mtime, &inode.Ctime, &inode.Crtime, &inode.SymLink)
	if err != nil {
		return nil, syscall.ENOENT
	}

	inode.Mode = os.FileMode(mode)
	inode.ID = fuseops.InodeID(id)

	return &database.Entry{Inode: inode, Name: name, Parent: parent}, nil
}

// Get retrieves the stats of a particular inode
func (d *Driver) Get(ctx context.Context, inode fuseops.InodeID) (*database.Inode, error) {
	var mode uint32

	row := d.DB.QueryRowContext(ctx, "SELECT mode, uid, gid, size, refcount, atime, mtime, ctime, crtime, target FROM inodes WHERE id = ?", uint64(inode))

	result := database.Inode{}
	result.ID = inode

	err := row.Scan(&mode, &result.Uid, &result.Gid, &result.Size, &result.Nlink, &result.Atime, &result.Mtime, &result.Ctime, &result.Crtime, &result.SymLink)
	if err != nil {
		return nil, syscall.ENOENT
	}

	result.Mode = os.FileMode(mode)
	return &result, nil
}

// GetAll retrieves the stats of requested Inodes
func (d *Driver) GetAll(ctx context.Context, inodes []fuseops.InodeID) (*[]database.Inode, error) {
	values := make([]string, 0)

	for _, value := range inodes {
		values = append(values, strconv.FormatUint(uint64(value), 10))
	}

	rows, err := d.DB.QueryContext(ctx, "SELECT id, mode, uid, gid, size, refcount, atime, mtime, ctime, crtime, target FROM inodes WHERE id IN ("+strings.Join(values, ", ")+")")
	if err != nil {
		return nil, treatError(err)
	}

	inodeList := make([]database.Inode, 0)

	for rows.Next() {
		var id uint64
		var mode uint32

		result := database.Inode{}

		err = rows.Scan(&id, &mode, &result.Uid, &result.Gid, &result.Size, &result.Nlink, &result.Atime, &result.Mtime, &result.Ctime, &result.Crtime, &result.SymLink)
		if err != nil {
			return nil, treatError(err)
		}

		result.ID = fuseops.InodeID(id)
		result.Mode = os.FileMode(mode)
		inodeList = append(inodeList, result)
	}

	return &inodeList, nil
}

// Touch changes the stats of a file
func (d *Driver) Touch(ctx context.Context, inode fuseops.InodeID, size *uint64, mode *os.FileMode, atime *time.Time, mtime *time.Time, uid *uint32, gid *uint32) (*database.Inode, error) {
	chunksToBeDeleted := make([]string, 0)

	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, treatError(err)
	}

	i, err := d.getInode(tx, inode)
	if err != nil {
		tx.Rollback()
		return nil, treatError(err)
	}

	if size != nil && *size != i.Size {

		if *size > i.Size {
			if _, err = tx.Exec("INSERT INTO chunks(inode, storage, objectoffset, inodeoffset, size) VALUES (?, 'zero', 0, ?, ?)", uint64(i.ID), i.Size, *size-i.Size); err != nil {
				tx.Rollback()
				return nil, treatError(err)
			}

			if _, err = tx.Exec("UPDATE stats SET size = size + ?", *size-i.Size); err != nil {
				tx.Rollback()
				return nil, treatError(err)
			}
		} else {
			var rows *sql.Rows

			rows, err = tx.Query("SELECT id, storage, `key`, objectoffset, inodeoffset, size FROM chunks WHERE inode = ? AND inodeoffset + size > ?", uint64(i.ID), *size)
			if err != nil {
				tx.Rollback()
				return nil, treatError(err)
			}

			defer rows.Close()

			for rows.Next() {

				chunk := database.Chunk{Inode: i.ID}

				err = rows.Scan(
					&chunk.ID,
					&chunk.Storage,
					&chunk.Key,
					&chunk.ObjectOffset,
					&chunk.InodeOffset,
					&chunk.Size,
				)

				if err != nil {
					tx.Rollback()
					return nil, treatError(err)
				}

				if chunk.InodeOffset < *size {
					if _, err = tx.Exec("UPDATE chunks SET size = ? WHERE id = ?", *size-chunk.InodeOffset, chunk.ID); err != nil {
						tx.Rollback()
						return nil, treatError(err)
					}
				} else {
					chunksToBeDeleted = append(chunksToBeDeleted, strconv.FormatUint(chunk.ID, 10))
				}

			}

			if _, err = tx.Exec("UPDATE stats SET size = size - ?", i.Size-*size); err != nil {
				tx.Rollback()
				return nil, treatError(err)
			}
		}

		i.Size = *size
	}

	if mode != nil {
		i.Mode = *mode
	}

	if atime != nil {
		i.Atime = *atime
	}

	if mtime != nil {
		i.Mtime = *mtime
	}

	if uid != nil {
		i.Uid = *uid
	}

	if gid != nil {
		i.Gid = *gid
	}

	if _, err = tx.Exec("UPDATE inodes SET mode = ?, uid = ?, gid = ?, size = ?, atime = ?, mtime = ?, ctime = NOW() WHERE id = ?", uint32(i.Mode), i.Uid, i.Gid, i.Size, i.Atime, i.Mtime, uint64(i.ID)); err != nil {
		tx.Rollback()
		return nil, treatError(err)
	}

	if len(chunksToBeDeleted) > 0 {
		if _, err = tx.Exec("UPDATE chunks SET inode = NULL, objectoffset = NULL, inodeoffset = NULL, size = NULL, orphandate = NOW() WHERE id IN (" + strings.Join(chunksToBeDeleted, ", ") + ")"); err != nil {
			tx.Rollback()
			return nil, treatError(err)
		}
	}

	if err = tx.Commit(); err != nil {
		return nil, treatError(err)
	}

	return i, nil
}

// AddChunk adds a chunk to the given inode
func (d *Driver) AddChunk(ctx context.Context, inode fuseops.InodeID, chunk database.Chunk) error {
	chunksToBeDeleted := make([]string, 0)

	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return treatError(err)
	}

	i, err := d.getInode(tx, inode)
	if err != nil {
		tx.Rollback()
		return treatError(err)
	}

	if i.Size < chunk.InodeOffset {
		if _, err = tx.Exec("INSERT INTO chunks(inode, storage, objectoffset, inodeoffset, size) VALUES (?, 'zero', 0, ?, ?)", uint64(i.ID), i.Size, chunk.InodeOffset-i.Size); err != nil {
			tx.Rollback()
			return treatError(err)
		}
	}

	rows, err := tx.Query("SELECT id, storage, `key`, objectoffset, inodeoffset, size FROM chunks WHERE inode = ? AND inodeoffset < ? AND inodeoffset + size > ?", uint64(inode), chunk.InodeOffset+chunk.Size, chunk.InodeOffset)
	if err != nil {
		tx.Rollback()
		return treatError(err)
	}

	defer rows.Close()

	for rows.Next() {

		c := database.Chunk{Inode: inode}

		err = rows.Scan(
			&c.ID,
			&c.Storage,
			&c.Key,
			&c.ObjectOffset,
			&c.InodeOffset,
			&c.Size,
		)

		if err != nil {
			tx.Rollback()
			return treatError(err)
		}

		if c.InodeOffset > chunk.InodeOffset && c.InodeOffset+c.Size < chunk.InodeOffset+c.Size {
			chunksToBeDeleted = append(chunksToBeDeleted, strconv.FormatUint(c.ID, 10))
		} else {
			newInodeOffset := math.Max(c.InodeOffset, chunk.InodeOffset)
			newInodeEnd := math.Min(c.InodeOffset+c.Size, chunk.InodeOffset+chunk.Size)

			c.ObjectOffset += newInodeOffset - c.InodeOffset
			c.InodeOffset = newInodeOffset
			c.Size = newInodeEnd - c.InodeOffset

			if _, err = tx.Exec("UPDATE chunks SET size = ?, inodeoffset = ?, objectoffset = ? WHERE id = ?", c.Size, c.InodeOffset, c.ObjectOffset, c.ID); err != nil {
				tx.Rollback()
				return treatError(err)
			}
		}

	}

	_, err = tx.Exec("INSERT INTO chunks(inode, storage, `key`, objectoffset, inodeoffset, size) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", uint64(inode), chunk.Storage, chunk.Key, chunk.ObjectOffset, chunk.InodeOffset, chunk.Size)
	if err != nil {
		tx.Rollback()
		return treatError(err)
	}

	newInodeSize := math.Max(i.Size, chunk.InodeOffset+chunk.Size)

	if newInodeSize != i.Size {
		if _, err = tx.Exec("UPDATE stats SET size = size + ?", newInodeSize-i.Size); err != nil {
			tx.Rollback()
			return treatError(err)
		}

		i.Size = newInodeSize
	}

	if _, err = tx.Exec("UPDATE inodes SET size = ?, atime = NOW(), mtime = NOW(), ctime = NOW() WHERE id = ?", i.Size, uint64(i.ID)); err != nil {
		tx.Rollback()
		return treatError(err)
	}

	if len(chunksToBeDeleted) > 0 {
		if _, err = tx.Exec("UPDATE chunks SET inode = NULL, objectoffset = NULL, inodeoffset = NULL, size = NULL, orphandate = NOW() WHERE id IN (" + strings.Join(chunksToBeDeleted, ", ") + ")"); err != nil {
			tx.Rollback()
			return treatError(err)
		}
	}

	if err = tx.Commit(); err != nil {
		return treatError(err)
	}

	return nil
}

// Chunks grabs the chunks for the given inode
func (d *Driver) Chunks(ctx context.Context, inode fuseops.InodeID) (*[]database.Chunk, error) {
	if _, err := d.DB.ExecContext(ctx, "UPDATE inodes SET atime = NOW() WHERE id = ?", uint64(inode)); err != nil {
		return nil, treatError(err)
	}

	rows, err := d.DB.QueryContext(ctx, "SELECT id, storage, `key`, objectoffset, inodeoffset, size FROM chunks WHERE inode = ? ORDER BY inodeoffset ASC", uint64(inode))
	if err != nil {
		return nil, treatError(err)
	}

	chunks := make([]database.Chunk, 0)

	for rows.Next() {
		chunk := database.Chunk{Inode: inode}

		err := rows.Scan(
			&chunk.ID,
			&chunk.Storage,
			&chunk.Key,
			&chunk.ObjectOffset,
			&chunk.InodeOffset,
			&chunk.Size,
		)

		if err != nil {
			return nil, err
		}

		chunks = append(chunks, chunk)
	}

	return &chunks, nil
}

// Children gets the list of children for the given inode
func (d *Driver) Children(ctx context.Context, inode fuseops.InodeID) (*[]database.Child, error) {
	if _, err := d.DB.ExecContext(ctx, "UPDATE inodes SET atime = NOW() WHERE id = ?", uint64(inode)); err != nil {
		return nil, treatError(err)
	}

	rows, err := d.DB.QueryContext(ctx, "SELECT e.inode, e.name, i.mode FROM entries e, inodes i WHERE e.parent = ? AND i.id = e.inode", uint64(inode))
	if err != nil {
		return nil, treatError(err)
	}

	children := make([]database.Child, 0)

	for rows.Next() {
		var inode uint64
		var mode uint32
		var name string

		err := rows.Scan(
			&inode,
			&name,
			&mode,
		)

		if err != nil {
			return nil, err
		}

		child := database.Child{
			Inode: fuseops.InodeID(inode),
			Name:  name,
			Mode:  os.FileMode(mode),
		}

		children = append(children, child)
	}

	return &children, nil
}

// ListXattr retrieves the list of extended attributes for the given inode
func (d *Driver) ListXattr(ctx context.Context, inode fuseops.InodeID) (*[]string, error) {
	keys := make([]string, 0)

	rows, err := d.DB.QueryContext(ctx, "SELECT `key` FROM xattr WHERE inode = ?")
	if err != nil {
		return nil, treatError(err)
	}

	for rows.Next() {
		var key string

		if err = rows.Scan(&key); err != nil {
			return nil, treatError(err)
		}

		keys = append(keys, key)
	}

	return &keys, nil
}

// RemoveXattr removes the given extended attribute from the given inode
func (d *Driver) RemoveXattr(ctx context.Context, inode fuseops.InodeID, attr string) error {
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return treatError(err)
	}

	if _, err := tx.Exec("DELETE FROM xattr WHERE inode = ? AND `key` = ?", uint64(inode), attr); err != nil {
		tx.Rollback()
		return treatError(err)
	}

	if _, err := tx.Exec("UPDATE inodes SET ctime = NOW(), atime = NOW() WHERE id = ?", uint64(inode)); err != nil {
		tx.Rollback()
		return treatError(err)
	}

	return tx.Commit()
}

// GetXattr gets a certain external attribute from the given inode
func (d *Driver) GetXattr(ctx context.Context, inode fuseops.InodeID, attr string) (*[]byte, error) {
	row := d.DB.QueryRowContext(ctx, "SELECT value FROM xattr WHERE inode = ? AND `key` = ?", uint64(inode), attr)

	var data []byte
	if err := row.Scan(&data); err != nil {
		return nil, treatError(err)
	}

	return &data, nil
}

// SetXattr sets an extended attribute at the given node
func (d *Driver) SetXattr(ctx context.Context, inode fuseops.InodeID, attr string, value []byte, flags uint32) error {
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return treatError(err)
	}

	switch flags {
	case 0x1:

		if _, err = tx.Exec("INSERT INTO xattr(inode, `key`, value) VALUES (?, ?, ?)", uint64(inode), attr, value); err != nil {
			tx.Rollback()
			return treatError(err)
		}

	case 0x2:

		var result sql.Result
		var rowsAffected int64

		if result, err = tx.Exec("UPDATE xattr SET value = ? WHERE inode = ? AND `key` = ?", value, uint64(inode), attr); err != nil {
			tx.Rollback()
			return treatError(err)
		}

		if rowsAffected, err = result.RowsAffected(); err != nil {
			tx.Rollback()
			return treatError(err)
		}

		if rowsAffected == 0 {
			tx.Rollback()
			return syscall.ENODATA
		}

	default:

		if _, err = tx.Exec("INSERT INTO xattr(inode, `key`, value) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)", uint64(inode), attr, value); err != nil {
			tx.Rollback()
			return treatError(err)
		}

	}

	if _, err := tx.Exec("UPDATE inodes SET ctime = NOW(), atime = NOW() WHERE id = ?", uint64(inode)); err != nil {
		tx.Rollback()
		return treatError(err)
	}

	return tx.Commit()
}
