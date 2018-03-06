package main

import (
	"context"
	"log"
	"os"

	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "Titan FS"
	app.Usage = "FUSE filesystem"
	app.Description = "A FUSE filesystem backed by a DB and an Object Storage"
	app.Version = "1.0.0"

	flags := []cli.Flag{
		// DB options
		cli.StringFlag{
			Name:   "db-driver",
			Value:  "mysql",
			Usage:  "database driver",
			EnvVar: "TITAN_DB_DRIVER",
		},
		cli.StringFlag{
			Name:   "db-uri",
			Usage:  "database URI",
			Value:  "root:@/titan",
			EnvVar: "TITAN_DB_URI",
		},

		// Storage options
		cli.StringFlag{
			Name:   "storage-driver",
			Value:  "S3",
			Usage:  "storage driver",
			EnvVar: "TITAN_STORAGE_DRIVER",
		},

		// Cache options
		cli.StringFlag{
			Name:   "cache-folder",
			Value:  "/tmp/titan",
			Usage:  "folder to use when storing cache files",
			EnvVar: "TITAN_CACHE_FOLDER",
		},
	}

	app.Commands = []cli.Command{
		cli.Command{
			Name:  "mkfs",
			Flags: flags,
			Action: func(c *cli.Context) error {
				l := log.New(os.Stderr, "", 0)

				db, err := newDB(
					c.String("db-driver"),
					c.String("db-uri"),
				)

				if err != nil {
					l.Println(err)
					return err
				}

				defer db.Close()

				err = db.Setup(context.Background())

				if err != nil {
					l.Println(err)
					return err
				}

				return nil
			},
		},
	}

	app.Run(os.Args)
}
