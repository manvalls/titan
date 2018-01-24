package titan

import (
	"errors"

	"git.vlrz.es/manvalls/titan/database"
	"git.vlrz.es/manvalls/titan/database/mysql"
)

var errNotSup = errors.New("Database driver not supported")

// NewDB returns a database implementation using provided driver name
// and database URI
func NewDB(dbDriver string, dbURI string, eraser database.ChunkEraser) (database.Db, error) {

	switch dbDriver {
	case "mysql":
		return mysql.Driver{DbURI: dbURI, ChunkEraser: eraser}, nil
	}

	return nil, errNotSup
}
