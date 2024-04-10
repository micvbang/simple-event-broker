package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

type S3StorageInput struct {
	S3         s3iface.S3API
	BucketName string
	RootDir    string
	TopicName  string
	Cache      *DiskCache
}

type S3TopicStorage struct {
	log        logger.Logger
	s3         s3iface.S3API
	bucketName string
}

// NewS3TopicStorage returns a *TopicStorage that stores data in AWS S3.
func NewS3TopicStorage(log logger.Logger, input S3StorageInput) (*TopicStorage, error) {
	s3Storage := &S3TopicStorage{
		log:        log,
		s3:         input.S3,
		bucketName: input.BucketName,
	}

	return NewTopicStorage(log, s3Storage, input.RootDir, input.TopicName, input.Cache)
}

func (ss *S3TopicStorage) Writer(recordBatchPath string) (io.WriteCloser, error) {
	log := ss.log.WithField("recordBatchPath", recordBatchPath)

	log.Debugf("creating temp file")
	tmpFile, err := os.CreateTemp("", "seb_*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	log = log.WithField("temp file", tmpFile.Name())

	log.Debugf("creating s3WriteCloser")
	writeCloser := &s3WriteCloser{
		log:        ss.log.Name("s3UploadWriteCloser"),
		f:          tmpFile,
		s3:         ss.s3,
		bucketName: ss.bucketName,
		objectKey:  recordBatchPath,
	}

	return writeCloser, nil
}

func (ss *S3TopicStorage) Reader(recordBatchPath string) (io.ReadCloser, error) {
	log := ss.log.WithField("recordBatchPath", recordBatchPath)

	log.Debugf("fetching record batch from s3")
	obj, err := ss.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(ss.bucketName),
		Key:    &recordBatchPath,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				err = errors.Join(err, ErrNotInStorage)
			}
		}

		return nil, fmt.Errorf("retrieving s3 object: %w", err)
	}

	// NOTE: intentionally not closing obj.Body, this is caller's responsibility
	return obj.Body, nil
}

func (ss *S3TopicStorage) ListFiles(topicPath string, extension string) ([]File, error) {
	log := ss.log.
		WithField("topicPath", topicPath).
		WithField("extension", extension)

	files := make([]File, 0, 128)

	topicPath, _ = strings.CutPrefix(topicPath, "/")
	if !strings.HasSuffix(topicPath, "/") {
		topicPath += "/"
	}

	log.Debugf("listing objects in s3")
	t0 := time.Now()
	err := ss.s3.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(ss.bucketName),
		Prefix: &topicPath,
	}, func(objects *s3.ListObjectsOutput, b bool) bool {
		for _, obj := range objects.Contents {
			if obj == nil || obj.Key == nil {
				continue
			}

			filePath := *obj.Key

			if strings.HasSuffix(filePath, extension) {
				files = append(files, File{
					Path: filePath,
					Size: *obj.Size,
				})
			}
		}
		return true
	})

	log.Debugf("found %d files (%s)", len(files), time.Since(t0))

	return files, err
}

type s3WriteCloser struct {
	log logger.Logger
	s3  s3iface.S3API

	f          *os.File
	bucketName string
	objectKey  string
}

func (wc *s3WriteCloser) Write(b []byte) (int, error) {
	return wc.f.Write(b)
}

func (wc *s3WriteCloser) Close() error {
	_, err := wc.f.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seeking to beginning: %w", err)
	}

	wc.log.Debugf("uploading to %s%s", wc.bucketName, wc.objectKey)
	t0 := time.Now()
	_, err = wc.s3.PutObject(&s3.PutObjectInput{
		Bucket: &wc.bucketName,
		Key:    &wc.objectKey,
		Body:   wc.f,
	})
	if err != nil {
		return fmt.Errorf("uploading to s3: %w", err)
	}
	wc.log.Debugf("uploaded to %s%s (%s)", wc.bucketName, wc.objectKey, time.Since(t0))

	return wc.f.Close()
}
