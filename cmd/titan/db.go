package main

import (
	"errors"

	"github.com/manvalls/titan/database"
	"github.com/manvalls/titan/database/mysql"
	"github.com/urfave/cli"
)

var errDbNotSup = errors.New("Database driver not supported")

func newDB(c *cli.Context) (database.Db, error) {
	var db database.Db

	switch c.String("db-driver") {
	case "mysql":
		db = &mysql.Driver{DbURI: c.String("db-uri")}
	default:
		return nil, errDbNotSup
	}

	err := db.Open()
	if err != nil {
		return nil, err
	}

	return db, nil
}
