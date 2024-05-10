package topic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

// S3Storage is an Amazon S3 backing storage that can be used in Topic.
type S3Storage struct {
	log         logger.Logger
	s3          S3API
	bucketName  string
	s3KeyPrefix string
}

type S3API interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

func NewS3Storage(log logger.Logger, s3 S3API, bucketName string, s3KeyPrefix string) *S3Storage {
	return &S3Storage{
		log:         log,
		s3:          s3,
		bucketName:  bucketName,
		s3KeyPrefix: s3KeyPrefix,
	}
}

func (ss *S3Storage) Writer(key string) (io.WriteCloser, error) {
	log := ss.log.WithField("recordBatchPath", key)

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
		objectKey:  path.Join(ss.s3KeyPrefix, key),
	}

	return writeCloser, nil
}

func (ss *S3Storage) Reader(key string) (io.ReadCloser, error) {
	log := ss.log.WithField("recordBatchPath", key)

	log.Debugf("fetching record batch from s3")
	obj, err := ss.s3.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(ss.bucketName),
		Key:    aws.String(path.Join(ss.s3KeyPrefix, key)),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorCode() == "NoSuchKey" {
				err = errors.Join(err, seb.ErrNotInStorage)
			}
		}

		return nil, fmt.Errorf("retrieving s3 object: %w", err)
	}

	// NOTE: intentionally not closing obj.Body, this is caller's responsibility
	return obj.Body, nil
}

func (ss *S3Storage) ListFiles(topicName string, extension string) ([]File, error) {
	log := ss.log.
		WithField("topicPath", topicName).
		WithField("extension", extension)

	topicName = path.Join(ss.s3KeyPrefix, topicName)
	topicName, _ = strings.CutPrefix(topicName, "/")
	if !strings.HasSuffix(topicName, "/") {
		topicName += "/"
	}

	log.Debugf("listing objects in s3")
	t0 := time.Now()

	files := make([]File, 0, 128)
	paginator := s3.NewListObjectsV2Paginator(ss.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(ss.bucketName),
		Prefix: &topicName,
	})
	for paginator.HasMorePages() {
		result, err := paginator.NextPage(context.TODO())
		if err != nil {
			err = fmt.Errorf("retrieving pages: %w", err)
			log.Errorf(err.Error())
			return nil, err
		}

		for _, obj := range result.Contents {
			if obj.Key == nil {
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
	}

	log.Debugf("found %d files (%s)", len(files), time.Since(t0))

	return files, nil
}

type s3WriteCloser struct {
	log logger.Logger
	s3  S3API

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

	wc.log.Debugf("uploading to s3://%s/%s", wc.bucketName, wc.objectKey)
	t0 := time.Now()
	_, err = wc.s3.PutObject(context.Background(), &s3.PutObjectInput{
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
