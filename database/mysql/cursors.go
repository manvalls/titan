package mysql

import (
	"database/sql"
	"io"
	"os"

	"git.vlrz.es/manvalls/titan/database"
	"github.com/jacobsa/fuse/fuseops"
)

type chunkCursor struct {
	rows  *sql.Rows
	inode fuseops.InodeID
}

func (c *chunkCursor) Close() error {
	return c.rows.Close()
}

func (c *chunkCursor) Next() (*database.Chunk, error) {
	ok := c.rows.Next()
	if !ok {
		return nil, io.EOF
	}

	chunk := database.Chunk{Inode: c.inode}

	err := c.rows.Scan(
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

	return &chunk, nil
}

type childCursor struct {
	rows *sql.Rows
}

func (c *childCursor) Close() error {
	return c.rows.Close()
}

func (c *childCursor) Next() (*database.Child, error) {
	ok := c.rows.Next()
	if !ok {
		return nil, io.EOF
	}

	var inode uint64
	var mode uint32
	var name string

	err := c.rows.Scan(
		&inode,
		&name,
		&mode,
	)

	if err != nil {
		return nil, err
	}

	return &database.Child{
		Inode: fuseops.InodeID(inode),
		Name:  name,
		Mode:  os.FileMode(mode),
	}, nil
}
