package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/manvalls/fuse"
	"github.com/manvalls/titan"
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

		cli.Command{
			Name: "clean",
			Flags: append(
				flags,
				cli.DurationFlag{
					Name:   "keep-last",
					Value:  0 * time.Second,
					Usage:  "keep chunks which were orphaned the last X days",
					EnvVar: "TITAN_CLEAN_KEEP_LAST",
				},
				cli.IntFlag{
					Name:   "workers",
					Value:  10,
					Usage:  "number of parallel workers",
					EnvVar: "TITAN_CLEAN_WORKERS",
				},
			),
			Action: func(c *cli.Context) error {
				l := log.New(os.Stderr, "", 0)

				db, err := newDB(c)
				if err != nil {
					l.Println(err)
					return err
				}

				defer db.Close()

				err = db.CleanOrphanInodes(context.Background())
				if err != nil {
					l.Println(err)
					return err
				}

				st, err := newStorage(c)
				if err != nil {
					l.Println(err)
					return err
				}

				err = db.CleanOrphanChunks(
					context.Background(),
					time.Now().Add(-c.Duration("keep-last")),
					st,
					c.Int("workers"),
				)

				return err
			},
		},

		cli.Command{
			Name: "mount",
			Flags: append(
				flags,

				// Mount options
				cli.StringFlag{
					Name:   "mount-point",
					Value:  "/titan",
					Usage:  "folder to mount titan in",
					EnvVar: "TITAN_MOUNT_POINT",
				},
				cli.StringFlag{
					Name:   "cache-folder",
					Value:  "/tmp/titan",
					Usage:  "folder to use when storing cache files",
					EnvVar: "TITAN_CACHE_FOLDER",
				},
				cli.DurationFlag{
					Name:   "prune-interval",
					Value:  2 * time.Minute,
					Usage:  "prune interval",
					EnvVar: "TITAN_PRUNE_INTERVAL",
				},
				cli.DurationFlag{
					Name:   "inactivity-timeout",
					Value:  20 * time.Second,
					Usage:  "inactivity timeout",
					EnvVar: "TITAN_INACTIVITY_TIMEOUT",
				},
				cli.DurationFlag{
					Name:   "readahead-timeout",
					Value:  100 * time.Millisecond,
					Usage:  "readahead timeout",
					EnvVar: "TITAN_READAHEAD_TIMEOUT",
				},
				cli.DurationFlag{
					Name:   "cache-timeout",
					Value:  1 * time.Hour,
					Usage:  "cache timeout",
					EnvVar: "TITAN_CACHE_TIMEOUT",
				},
				cli.Uint64Flag{
					Name:   "disk-threshold",
					Value:  10 * 1e9,
					Usage:  "free disk space threshold",
					EnvVar: "TITAN_DISK_THRESHOLD",
				},
				cli.Uint64Flag{
					Name:   "max-cache-inodes",
					Value:  10e3,
					Usage:  "max number of inodes in the cache",
					EnvVar: "TITAN_CACHE_MAX_INODES",
				},
				cli.UintFlag{
					Name:   "cache-buffer",
					Value:  65536,
					Usage:  "size of the cache buffer",
					EnvVar: "TITAN_CACHE_BUFFER",
				},
				cli.Uint64Flag{
					Name:   "max-offset-distance",
					Value:  100e3,
					Usage:  "max allowed offset distance without a refetch",
					EnvVar: "TITAN_MAX_OFFSET_DISTANCE",
				},
				cli.DurationFlag{
					Name:   "attr-exp",
					Value:  10 * time.Second,
					Usage:  "attributes expiration",
					EnvVar: "TITAN_ATTR_EXP",
				},
				cli.DurationFlag{
					Name:   "entry-exp",
					Value:  10 * time.Second,
					Usage:  "entry expiration",
					EnvVar: "TITAN_ENTRY_EXP",
				},
				cli.Int64Flag{
					Name:   "max-chunk-size",
					Value:  134217728,
					Usage:  "max chunk size",
					EnvVar: "TITAN_MAX_CHUNK_SIZE",
				},
				cli.BoolFlag{
					Name:   "enable-capabilities",
					Usage:  "enable file capabilities",
					EnvVar: "TITAN_ENABLE_CAPABILITIES",
				},
			),
			Action: func(c *cli.Context) error {
				l := log.New(os.Stderr, "", 0)

				db, err := newDB(c)
				if err != nil {
					l.Println(err)
					return err
				}

				defer db.Close()

				err = db.CleanOrphanInodes(context.Background())
				if err != nil {
					l.Println(err)
					return err
				}

				st, err := newStorage(c)
				if err != nil {
					l.Println(err)
					return err
				}

				mountPoint := c.String("mount-point")
				fuse.Unmount(mountPoint)

				mfs, err := titan.Mount(mountPoint, titan.MountOptions{
					Storage:       st,
					Db:            db,
					CacheLocation: c.String("cache-folder"),

					PruneInterval: func() *time.Duration {
						t := c.Duration("prune-interval")
						return &t
					}(),

					MaxOffsetDistance: func() *uint64 {
						t := c.Uint64("max-offset-distance")
						return &t
					}(),

					InactivityTimeout: func() *time.Duration {
						t := c.Duration("inactivity-timeout")
						return &t
					}(),

					ReadAheadTimeout: func() *time.Duration {
						t := c.Duration("readahead-timeout")
						return &t
					}(),

					CtimeCacheTimeout: func() *time.Duration {
						t := c.Duration("cache-timeout")
						return &t
					}(),

					FreeSpaceThreshold: func() *uint64 {
						t := c.Uint64("disk-threshold")
						return &t
					}(),

					MaxInodes: func() *uint64 {
						t := c.Uint64("max-cache-inodes")
						return &t
					}(),

					BufferSize: func() *uint32 {
						t := uint32(c.Uint("cache-buffer"))
						return &t
					}(),

					AttributesExpiration: func() *time.Duration {
						t := c.Duration("attr-exp")
						return &t
					}(),

					EntryExpiration: func() *time.Duration {
						t := c.Duration("entry-exp")
						return &t
					}(),

					MaxChunkSize: func() *int64 {
						t := c.Int64("max-chunk-size")
						return &t
					}(),

					EnableCapabilities: func() *bool {
						t := c.Bool("enable-capabilities")
						return &t
					}(),

					MountConfig: &fuse.MountConfig{
						DisableWritebackCaching: true,
						FSName:                  "titan",
						VolumeName:              "TitanFS",
						Options: map[string]string{
							"allow_other": "",
						},
					},
				})

				if err != nil {
					l.Println(err)
					return err
				}

				return mfs.Join(context.Background())
			},
		},
	}

	app.Run(os.Args)
}
