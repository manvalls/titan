package titan

import (
	"errors"

	"git.vlrz.es/manvalls/titan/database"
	"git.vlrz.es/manvalls/titan/database/mysql"
	"git.vlrz.es/manvalls/titan/storage"
)

var errNotSup = errors.New("Database driver not supported")

// NewDB returns a database implementation using provided driver name
// and database URI
func NewDB(dbDriver string, dbURI string, s storage.Storage) (database.Db, error) {
	var db database.Db

	switch dbDriver {
	case "mysql":
		db = &mysql.Driver{DbURI: dbURI, Storage: s}
	default:
		return nil, errNotSup
	}

	err := db.Open()
	if err != nil {
		return nil, err
	}

	return db, nil
}
