package main

import (
	"errors"

	"git.vlrz.es/manvalls/titan/storage"
	"git.vlrz.es/manvalls/titan/storage/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/urfave/cli"
)

var errStorageNotSup = errors.New("Storage driver not supported")

func newStorage(c *cli.Context) (storage.Storage, error) {
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
			return err
		}

		storage := &s3.S3{
			Storage: c.String("storage-name"),
			Bucket:  c.String("s3-bucket"),
			Client:  s3.New(session),
		}

		return storage, nil

	default:
		return nil, errStorageNotSup

	}
}
