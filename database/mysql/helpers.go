package mysql

import (
	"database/sql"
	"os"
	"syscall"

	"git.vlrz.es/manvalls/titan/database"
	"github.com/manvalls/fuse/fuseops"
)

func (d Driver) getInode(tx *sql.Tx, inode fuseops.InodeID) (*database.Inode, error) {
	var mode uint32

	row := tx.QueryRow("SELECT mode, uid, gid, size, refcount, atime, mtime, ctime, crtime, target FROM inodes WHERE id = ?", uint64(inode))

	result := database.Inode{}
	result.ID = inode

	err := row.Scan(&mode, &result.Uid, &result.Gid, &result.Size, &result.Nlink, &result.Atime, &result.Mtime, &result.Ctime, &result.Crtime, &result.SymLink)
	if err != nil {
		return nil, syscall.ENOENT
	}

	result.Mode = os.FileMode(mode)
	return &result, nil
}
