package mysql

import (
	"database/sql"
	"io"

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
		&chunk.Credentials,
		&chunk.Location,
		&chunk.Bucket,
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
