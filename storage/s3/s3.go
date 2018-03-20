package s3

import (
	"io"
	"strconv"

	"github.com/manvalls/titan/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3 is an AWS S3 implementation of the storage interface
type S3 struct {
	Storage string
	Client  *s3.S3
	Bucket  string
}

// Setup sets up the storage
func (s *S3) Setup() error {
	_, err := s.Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(s.Bucket),
	})

	return err
}

// GetChunk stores the contents of a reader and returns the built chunk
func (s *S3) GetChunk(reader io.Reader) (*storage.Chunk, error) {
	r := &storage.ReaderWithSize{Reader: reader}
	uploader := s3manager.NewUploaderWithClient(s.Client)

	filename, err := storage.Key()
	if err != nil {
		return nil, err
	}

	_, err = uploader.Upload(&s3manager.UploadInput{
		Body:   r,
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(filename),
	})

	if err != nil {
		return nil, err
	}

	return &storage.Chunk{
		Storage:      s.Storage,
		Key:          filename,
		ObjectOffset: 0,
		Size:         r.Size,
	}, err
}

// GetReadCloser retrieves the contents of a chunk
func (s *S3) GetReadCloser(chunk storage.Chunk) (io.ReadCloser, error) {
	result, err := s.Client.GetObject(&s3.GetObjectInput{
		Range:  aws.String("bytes=" + strconv.FormatUint(chunk.ObjectOffset, 10) + "-" + strconv.FormatUint(chunk.ObjectOffset+chunk.Size-1, 10)),
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(chunk.Key),
	})

	if err != nil {
		return nil, err
	}

	return result.Body, nil
}

// Remove removes a chunk from the storage
func (s *S3) Remove(chunk storage.Chunk) error {
	_, err := s.Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(chunk.Key),
	})

	return err
}
