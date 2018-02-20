package main

import (
	"errors"

	"git.vlrz.es/manvalls/titan/database"
	"git.vlrz.es/manvalls/titan/database/mysql"
	"git.vlrz.es/manvalls/titan/storage"
)

var errDbNotSup = errors.New("Database driver not supported")

func newDB(dbDriver string, dbURI string, s storage.Storage) (database.Db, error) {
	var db database.Db

	switch dbDriver {
	case "mysql":
		db = &mysql.Driver{DbURI: dbURI, Storage: s}
	default:
		return nil, errDbNotSup
	}

	err := db.Open()
	if err != nil {
		return nil, err
	}

	return db, nil
}
