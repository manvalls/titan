package mysql

import (
	"context"
	"database/sql"
	"os"
	"syscall"
	"time"

	"git.vlrz.es/manvalls/titan/database"

	// mysql driver for the sql package
	_ "github.com/go-sql-driver/mysql"
	"github.com/jacobsa/fuse/fuseops"
)

// Driver implements the Db interface for the titan file system
type Driver struct {
	DbURI string
	*database.ChunkEraser
	*sql.DB
}

// Open opens the underlying connection
func (d *Driver) Open() error {
	db, err := sql.Open("mysql", d.DbURI)
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
		"CREATE TABLE inodes ( id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, mode INT UNSIGNED NOT NULL, target VARBINARY(4096), size BIGINT UNSIGNED NOT NULL, refcount INT UNSIGNED NOT NULL, atime DATETIME NOT NULL, mtime DATETIME NOT NULL, ctime DATETIME NOT NULL, crtime DATETIME NOT NULL, PRIMARY KEY (id) )",

		"CREATE TABLE entries (parent BIGINT UNSIGNED NOT NULL, name VARBINARY(255) NOT NULL, inode BIGINT UNSIGNED NOT NULL, PRIMARY KEY (parent, name), INDEX (parent), INDEX (inode), FOREIGN KEY (parent) REFERENCES inodes(id), FOREIGN KEY (inode) REFERENCES inodes(id))",

		"CREATE TABLE chunks (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, inode BIGINT UNSIGNED NOT NULL, storage VARCHAR(255), credentials VARCHAR(255), location VARCHAR(255), bucket VARCHAR(255), `key` VARCHAR(255), objectoffset BIGINT NOT NULL, inodeoffset BIGINT NOT NULL, size BIGINT NOT NULL, PRIMARY KEY (id), INDEX (inode), FOREIGN KEY (inode) REFERENCES inodes(id))",

		"CREATE TABLE xattr (inode BIGINT UNSIGNED NOT NULL, `key` VARBINARY(255), value VARBINARY(4096), PRIMARY KEY (inode, `key`), INDEX (inode), FOREIGN KEY (inode) REFERENCES inodes(id))",

		"CREATE TABLE stats (inodes BIGINT UNSIGNED NOT NULL, size BIGINT UNSIGNED NOT NULL)",

		"INSERT INTO inodes(id, mode, size, refcount, atime, mtime, ctime, crtime) VALUES(1, 2147484159, 4096, 2, NOW(), NOW(), NOW(), NOW())",
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

	if entry.ID == 0 {
		var result sql.Result
		var size uint64
		var id int64

		if entry.Mode.IsDir() {
			size = 4096
		} else {
			size = 0
		}

		if _, err = tx.Exec("UPDATE stats SET inodes = inodes + 1, size = size + ?", size); err != nil {
			tx.Rollback()
			return nil, treatError(err)
		}

		result, err = tx.Exec("INSERT INTO inodes(mode, size, refcount, atime, mtime, ctime, crtime, target) VALUES(?, ?, 0, NOW(), NOW(), NOW(), NOW(), ?)", uint32(entry.Mode), size, entry.SymLink)
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

			_, err = tx.Exec("INSERT INTO entries(parent, name, inode) VALUES(?, '.', ?), VALUES(?, '..', ?)", uint64(entry.ID), uint64(entry.ID), uint64(entry.ID), uint64(entry.Parent))
			if err != nil {
				tx.Rollback()
				return nil, treatError(err)
			}

			_, err = tx.Exec("UPDATE inodes SET refcount = refcount + 2 WHERE id = ?", uint64(entry.Parent))
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

	_, err = tx.Exec("UPDATE inodes SET refcount = refcount + 1 WHERE id = ?", uint64(entry.ID))
	if err != nil {
		tx.Rollback()
		return nil, treatError(err)
	}

	return &entry, nil
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

	chunks := make([]database.Chunk, 0, 1)

	if in.Nlink == 0 {
		var rows *sql.Rows

		rows, err = tx.Query("SELECT id, storage, credentials, location, bucket, `key`, objectoffset, inodeoffset, size FROM chunks WHERE inode = ?", in.ID)
		if err != nil {
			tx.Rollback()
			return treatError(err)
		}

		defer rows.Close()

		for rows.Next() {
			chunk := database.Chunk{Inode: in.ID}

			err = rows.Scan(
				&chunk.ID,
				&chunk.Storage,
				&chunk.Credentials,
				&chunk.Location,
				&chunk.Bucket,
				&chunk.Key,
				&chunk.ObjectOffset,
				&chunk.InodeOffset,
				&chunk.Size,
			)

			if err != nil {
				tx.Rollback()
				return treatError(err)
			}

			chunks = append(chunks, chunk)
		}

		if _, err = tx.Exec("DELETE c, i FROM chunks c, inodes i WHERE c.inode = i.id AND i.id = ?", uint64(in.ID)); err != nil {
			tx.Rollback()
			return treatError(err)
		}

		if _, err = tx.Exec("UPDATE stats SET size = size - ?, inodes = inodes - 1", in.Size); err != nil {
			tx.Rollback()
			return treatError(err)
		}

	}

	if err = tx.Commit(); err != nil {
		return treatError(err)
	}

	for _, chunk := range chunks {
		d.Erase(chunk)
	}

	return nil
}

// CleanOrphanInodes removes all orphan inodes and chunks
func (d *Driver) CleanOrphanInodes(ctx context.Context) error {
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return treatError(err)
	}

	chunks := make([]database.Chunk, 0, 1)

	rows, err := tx.Query("SELECT c.id, c.storage, c.credentials, c.location, c.bucket, c.key, c.objectoffset, c.inodeoffset, c.size, c.inode FROM chunks c, inodes i WHERE c.inode = i.id AND i.refcount = 0")

	if err != nil {
		tx.Rollback()
		return treatError(err)
	}

	defer rows.Close()

	var size uint64

	for rows.Next() {
		chunk := database.Chunk{}

		err = rows.Scan(
			&chunk.ID,
			&chunk.Storage,
			&chunk.Credentials,
			&chunk.Location,
			&chunk.Bucket,
			&chunk.Key,
			&chunk.ObjectOffset,
			&chunk.InodeOffset,
			&chunk.Size,
			&chunk.Inode,
		)

		if err != nil {
			tx.Rollback()
			return treatError(err)
		}

		size += chunk.Size
		chunks = append(chunks, chunk)
	}

	if _, err = tx.Exec("DELETE c FROM chunks c, inodes i WHERE c.inode = i.id AND i.refcount = 0"); err != nil {
		tx.Rollback()
		return treatError(err)
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

	if err = tx.Commit(); err != nil {
		return treatError(err)
	}

	for _, chunk := range chunks {
		d.Erase(chunk)
	}

	return nil
}

// Unlink removes an entry from the file system
func (d *Driver) Unlink(ctx context.Context, parent fuseops.InodeID, name string, removeDots bool) error {
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

	if removeDots {
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

	if removeDots {
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
		if _, err = tx.Exec("DELETE FROM entries WHERE inode = ?", uint64(inode)); err != nil {
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
	row := d.DB.QueryRowContext(ctx, "SELECT i.id, i.mode, i.size, i.refcount, i.atime, i.mtime, i.ctime, i.crtime, i.target FROM inodes i, entries e WHERE i.id = e.inode AND e.parent = ? AND e.name = ?", uint64(parent), name)

	var mode, id uint64
	inode := database.Inode{}

	err := row.Scan(&id, &mode, &inode.Size, &inode.Nlink, &inode.Atime, &inode.Mtime, &inode.Ctime, &inode.Crtime, &inode.SymLink)
	if err != nil {
		return nil, syscall.ENOENT
	}

	inode.Mode = os.FileMode(mode)
	inode.ID = fuseops.InodeID(id)

	return &database.Entry{Inode: inode, Name: name, Parent: parent}, nil
}

// Get retrieves the stats of a particular inode
func (d *Driver) Get(ctx context.Context, inode fuseops.InodeID) (*database.Inode, error) {
	var mode uint64

	row := d.DB.QueryRowContext(ctx, "SELECT mode, size, refcount, atime, mtime, ctime, crtime, target FROM inodes WHERE id = ?", uint64(inode))

	result := database.Inode{}
	result.ID = inode

	err := row.Scan(&mode, &result.Size, &result.Nlink, &result.Atime, &result.Mtime, &result.Ctime, &result.Crtime, &result.SymLink)
	if err != nil {
		return nil, syscall.ENOENT
	}

	result.Mode = os.FileMode(mode)
	return &result, nil
}

// Touch changes the stats of a file
func (d *Driver) Touch(ctx context.Context, inode fuseops.InodeID, size *uint64, mode *os.FileMode, atime *time.Time, mtime *time.Time) (*database.Inode, error) {
	chunksToBeDeleted := make([]database.Chunk, 0)

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

			rows, err = tx.Query("SELECT id, storage, credentials, location, bucket, `key`, objectoffset, inodeoffset, size FROM chunks WHERE inode = ? AND inodeoffset + size > ?", uint64(i.ID), *size)
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
					&chunk.Credentials,
					&chunk.Location,
					&chunk.Bucket,
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
					chunksToBeDeleted = append(chunksToBeDeleted, chunk)
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

	if _, err = tx.Exec("UPDATE inodes SET mode = ?, size = ?, atime = ?, mtime = ?, ctime = NOW() WHERE id = ?", uint32(i.Mode), i.Size, i.Atime, i.Mtime, uint64(i.ID)); err != nil {
		tx.Rollback()
		return nil, treatError(err)
	}

	if err = tx.Commit(); err != nil {
		return nil, treatError(err)
	}

	for _, chunk := range chunksToBeDeleted {
		d.Erase(chunk)
	}

	return i, nil
}

// AddChunk adds a chunk to the given inode
func (d *Driver) AddChunk(ctx context.Context, inode fuseops.InodeID, chunk database.Chunk) error {
	chunksToBeDeleted := make([]database.Chunk, 0)

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

	rows, err := tx.Query("SELECT id, storage, credentials, location, bucket, `key`, objectoffset, inodeoffset, size FROM chunks WHERE inode = ? AND inodeoffset < ? AND inodeoffset + size > ?", uint64(inode), chunk.InodeOffset+chunk.Size, chunk.InodeOffset)
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
			&c.Credentials,
			&c.Location,
			&c.Bucket,
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
			chunksToBeDeleted = append(chunksToBeDeleted, c)
		} else {
			newInodeOffset := max(c.InodeOffset, chunk.InodeOffset)
			newInodeEnd := min(c.InodeOffset+c.Size, chunk.InodeOffset+chunk.Size)

			c.ObjectOffset += newInodeOffset - c.InodeOffset
			c.InodeOffset = newInodeOffset
			c.Size = newInodeEnd - c.InodeOffset

			if _, err = tx.Exec("UPDATE chunks SET size = ?, inodeoffset = ?, objectoffset = ? WHERE id = ?", c.Size, c.InodeOffset, c.ObjectOffset, c.ID); err != nil {
				tx.Rollback()
				return treatError(err)
			}
		}

	}

	_, err = tx.Exec("INSERT INTO chunks(inode, storage, credentials, location, bucket, `key`, objectoffset, inodeoffset, size) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", uint64(inode), chunk.Storage, chunk.Credentials, chunk.Location, chunk.Bucket, chunk.Key, chunk.ObjectOffset, chunk.InodeOffset, chunk.Size)
	if err != nil {
		tx.Rollback()
		return treatError(err)
	}

	newInodeSize := max(i.Size, chunk.InodeOffset+chunk.Size)

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

	if err = tx.Commit(); err != nil {
		return treatError(err)
	}

	for _, chunk := range chunksToBeDeleted {
		d.Erase(chunk)
	}

	return nil
}

// Chunks grabs the chunks for the given inode, starting at the given offset
func (d *Driver) Chunks(ctx context.Context, inode fuseops.InodeID, offset uint64) (database.ChunkCursor, error) {
	if _, err := d.DB.ExecContext(ctx, "UPDATE inodes SET atime = NOW() WHERE id = ?", uint64(inode)); err != nil {
		return nil, treatError(err)
	}

	rows, err := d.DB.QueryContext(ctx, "SELECT id, storage, credentials, location, bucket, `key`, objectoffset, inodeoffset, size FROM chunks WHERE inode = ? AND inodeoffset + size > ? ORDER BY inodeoffset ASC", uint64(inode), offset)
	if err != nil {
		return nil, treatError(err)
	}

	return &chunkCursor{rows: rows, inode: inode}, nil
}

// Children gets the list of children for the given inode
func (d *Driver) Children(ctx context.Context, inode fuseops.InodeID, offset uint64) (database.ChildCursor, error) {
	if _, err := d.DB.ExecContext(ctx, "UPDATE inodes SET atime = NOW() WHERE id = ?", uint64(inode)); err != nil {
		return nil, treatError(err)
	}

	rows, err := d.DB.QueryContext(ctx, "SELECT e.inode, e.name, i.mode FROM entries e, inodes i WHERE e.parent = ? AND i.id = e.inode OFFSET ?", uint64(inode), offset)
	if err != nil {
		return nil, treatError(err)
	}

	return &childCursor{rows: rows}, nil
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
