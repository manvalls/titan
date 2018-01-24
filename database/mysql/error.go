package mysql

import (
	"syscall"

	"github.com/go-sql-driver/mysql"
)

func treatError(err error) error {
	me, ok := err.(*mysql.MySQLError)
	if !ok {
		return err
	}

	switch me.Number {
	case 1062:
		return syscall.EEXIST
	default:
		return err
	}
}
