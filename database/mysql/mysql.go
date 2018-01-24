package mysql

import (
	"context"
	"database/sql"
	"os"
	"syscall"

	"git.vlrz.es/manvalls/titan/database"

	// mysql driver for the sql package
	_ "github.com/go-sql-driver/mysql"
	"github.com/jacobsa/fuse/fuseops"
)

// Driver implements the Db interface for the titan file system
type Driver struct {
	DbURI string
	database.ChunkEraser
}

// Setup creates the tables and the initial data required by the file system
func (d Driver) Setup(ctx context.Context) error {
	db, err := sql.Open("mysql", d.DbURI)
	if err != nil {
		return err
	}

	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)

	if err != nil {
		return err
	}

	queries := []string{
		"CREATE TABLE inodes ( id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, mode INT UNSIGNED NOT NULL, target VARBINARY(4096), size BIGINT UNSIGNED NOT NULL, refcount INT UNSIGNED NOT NULL, atime DATETIME NOT NULL, mtime DATETIME NOT NULL, ctime DATETIME NOT NULL, crtime DATETIME NOT NULL, PRIMARY KEY (id) )",

		"CREATE TABLE entries (parent BIGINT UNSIGNED NOT NULL, name VARBINARY(255) NOT NULL, inode BIGINT UNSIGNED NOT NULL, PRIMARY KEY (parent, name), INDEX (parent), INDEX (inode), FOREIGN KEY (parent) REFERENCES inodes(id), FOREIGN KEY (inode) REFERENCES inodes(id))",

		"CREATE TABLE chunks (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, inode BIGINT UNSIGNED NOT NULL, storage VARCHAR(255), credentials VARCHAR(255), location VARCHAR(255), bucket VARCHAR(255), `key` VARCHAR(255), objectoffset BIGINT NOT NULL, inodeoffset BIGINT NOT NULL, size BIGINT NOT NULL, PRIMARY KEY (id), FOREIGN KEY (inode) REFERENCES inodes(id))",

		"CREATE TABLE xattr (inode BIGINT UNSIGNED NOT NULL, `key` VARBINARY(255), value VARBINARY(4096), PRIMARY KEY (inode, `key`), FOREIGN KEY (inode) REFERENCES inodes(id))",

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
func (d Driver) Stats(ctx context.Context) (*database.Stats, error) {

	db, err := sql.Open("mysql", d.DbURI)
	if err != nil {
		return nil, treatError(err)
	}

	defer db.Close()

	stats := database.Stats{}
	row := db.QueryRowContext(ctx, "SELECT inodes, size FROM stats")
	err = row.Scan(&stats.Inodes, &stats.Size)

	if err != nil {
		return nil, treatError(err)
	}

	return &stats, nil
}

// Create creates a new inode or link
func (d Driver) Create(ctx context.Context, entry database.Entry) (*database.Entry, error) {
	db, err := sql.Open("mysql", d.DbURI)
	if err != nil {
		return nil, treatError(err)
	}

	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
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
func (d Driver) Forget(ctx context.Context, inode fuseops.InodeID) error {
	db, err := sql.Open("mysql", d.DbURI)
	if err != nil {
		return treatError(err)
	}

	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
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
func (d Driver) CleanOrphanInodes(ctx context.Context) error {
	db, err := sql.Open("mysql", d.DbURI)
	if err != nil {
		return treatError(err)
	}

	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
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

		chunks = append(chunks, chunk)
	}

	if _, err = tx.Exec("DELETE c, i FROM chunks c, inodes i WHERE c.inode = i.id AND i.refcount = 0"); err != nil {
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
func (d Driver) Unlink(ctx context.Context, parent fuseops.InodeID, name string, removeDots bool) error {
	db, err := sql.Open("mysql", d.DbURI)
	if err != nil {
		return treatError(err)
	}

	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
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
func (d Driver) Rename(ctx context.Context, oldParent fuseops.InodeID, oldName string, newParent fuseops.InodeID, newName string) error {
	db, err := sql.Open("mysql", d.DbURI)
	if err != nil {
		return treatError(err)
	}

	defer db.Close()

	result, err := db.Exec("UPDATE entries SET parent = ?, name = ? WHERE parent = ?, name = ?", uint64(newParent), newName, uint64(oldParent), oldName)

	if err != nil {
		return treatError(err)
	}

	if rowsAffected, _ := result.RowsAffected(); rowsAffected == 0 {
		return syscall.ENOENT
	}

	return nil
}

// LookUp finds the entry located under the specified parent with the specified name
func (d Driver) LookUp(ctx context.Context, parent fuseops.InodeID, name string) (*database.Entry, error) {
	db, err := sql.Open("mysql", d.DbURI)
	if err != nil {
		return nil, treatError(err)
	}

	defer db.Close()

	row := db.QueryRow("SELECT i.id, i.mode, i.size, i.refcount, i.atime, i.mtime, i.ctime, i.crtime, i.target FROM inodes i, entries e WHERE i.id = e.inode AND e.parent = ? AND e.name = ?", uint64(parent), name)

	var mode, id uint64
	inode := database.Inode{}

	err = row.Scan(&id, &mode, &inode.Size, &inode.Nlink, &inode.Atime, &inode.Mtime, &inode.Ctime, &inode.Crtime, &inode.SymLink)
	if err != nil {
		return nil, syscall.ENOENT
	}

	inode.Mode = os.FileMode(mode)
	inode.ID = fuseops.InodeID(id)

	return &database.Entry{Inode: inode, Name: name, Parent: parent}, nil
}
