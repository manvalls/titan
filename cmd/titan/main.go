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
			Value:  "s3",
			Usage:  "storage driver",
			EnvVar: "TITAN_STORAGE_DRIVER",
		},
		cli.StringFlag{
			Name:   "storage-name",
			Value:  "s3",
			Usage:  "storage name",
			EnvVar: "TITAN_STORAGE_NAME",
		},

		cli.StringFlag{
			Name:   "s3-bucket",
			Value:  "titan",
			Usage:  "S3 bucket",
			EnvVar: "TITAN_S3_BUCKET",
		},
		cli.StringFlag{
			Name:   "s3-region",
			Value:  "us-east-1",
			Usage:  "S3 region",
			EnvVar: "TITAN_S3_REGION",
		},
		cli.StringFlag{
			Name:   "s3-key",
			Value:  "",
			Usage:  "S3 key",
			EnvVar: "TITAN_S3_KEY",
		},
		cli.StringFlag{
			Name:   "s3-secret",
			Value:  "",
			Usage:  "S3 secret",
			EnvVar: "TITAN_S3_SECRET",
		},
		cli.StringFlag{
			Name:   "s3-token",
			Value:  "",
			Usage:  "S3 token",
			EnvVar: "TITAN_S3_TOKEN",
		},
		cli.StringFlag{
			Name:   "s3-endpoint",
			Value:  "",
			Usage:  "S3 endpoint",
			EnvVar: "TITAN_S3_ENDPOINT",
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

				db, err := newDB(c)
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

				st, err := newStorage(c)
				if err != nil {
					l.Println(err)
					return err
				}

				err = st.Setup()
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
