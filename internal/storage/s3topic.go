package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

// S3TopicStorage is an Amazon S3 backing storage that can be used in Topic.
type S3TopicStorage struct {
	log        logger.Logger
	s3         s3iface.S3API
	bucketName string
	keyPrefix  string
}

func NewS3TopicStorage(log logger.Logger, s3 s3iface.S3API, bucketName string, keyPrefix string) *S3TopicStorage {
	return &S3TopicStorage{
		log:        log,
		s3:         s3,
		bucketName: bucketName,
		keyPrefix:  keyPrefix,
	}
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
		objectKey:  path.Join(ss.keyPrefix, recordBatchPath),
	}

	return writeCloser, nil
}

func (ss *S3TopicStorage) Reader(recordBatchPath string) (io.ReadCloser, error) {
	log := ss.log.WithField("recordBatchPath", recordBatchPath)

	log.Debugf("fetching record batch from s3")
	obj, err := ss.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(ss.bucketName),
		Key:    aws.String(path.Join(ss.keyPrefix, recordBatchPath)),
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

func (ss *S3TopicStorage) ListFiles(topicName string, extension string) ([]File, error) {
	log := ss.log.
		WithField("topicPath", topicName).
		WithField("extension", extension)

	topicName = path.Join(ss.keyPrefix, topicName)
	topicName, _ = strings.CutPrefix(topicName, "/")
	if !strings.HasSuffix(topicName, "/") {
		topicName += "/"
	}

	log.Debugf("listing objects in s3")
	t0 := time.Now()

	files := make([]File, 0, 128)
	err := ss.s3.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(ss.bucketName),
		Prefix: &topicName,
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
