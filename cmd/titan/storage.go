package main

import (
	"errors"

	"git.vlrz.es/manvalls/titan/storage"
	"git.vlrz.es/manvalls/titan/storage/multi"
	"git.vlrz.es/manvalls/titan/storage/s3"
	"git.vlrz.es/manvalls/titan/storage/zero"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	as3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/urfave/cli"
)

var errStorageNotSup = errors.New("Storage driver not supported")

func newStorage(c *cli.Context) (st storage.Storage, err error) {
	storageName := c.String("storage-name")

	switch c.String("storage-driver") {

	case "s3":
		config := &aws.Config{
			Region: aws.String(c.String("s3-region")),
			Credentials: credentials.NewStaticCredentials(
				c.String("s3-key"),
				c.String("s3-secret"),
				c.String("s3-token"),
			),
		}

		endpoint := c.String("s3-endpoint")
		if endpoint != "" {
			config.Endpoint = aws.String(endpoint)
		}

		session, err := session.NewSession(config)
		if err != nil {
			return nil, err
		}

		st = &s3.S3{
			Storage: storageName,
			Bucket:  c.String("s3-bucket"),
			Client:  as3.New(session),
		}

	default:
		return nil, errStorageNotSup

	}

	return &multi.Multi{
		Storages: map[string]storage.Storage{
			storageName: st,
			"zero": &zero.Zero{
				Storage: "zero",
			},
		},
		Default: storageName,
	}, nil
}
