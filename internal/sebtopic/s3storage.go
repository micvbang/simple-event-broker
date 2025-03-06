package sebtopic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/klauspost/compress/gzip"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/seberr"
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
				err = errors.Join(err, seberr.ErrNotInStorage)
			}
		}

		return nil, fmt.Errorf("retrieving s3 object: %w", err)
	}

	// NOTE: intentionally not closing obj.Body, this is caller's responsibility
	return obj.Body, nil
}

// ListFiles returns record batch files for topicName with the given extension.
//
// NOTE: As a performance optimization, ListFiles utilizes so-called overview
// files (e.g. "overviewXXX.json.gz") to avoid having to LIST all files in
// topicName
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

	filesFromOverview, err := ss.listFilesFromOverview(topicName)
	if err != nil {
		return nil, err
	}

	var mostRecentOverviewFile *string
	if len(filesFromOverview) > 0 {
		mostRecentOverviewFile = &filesFromOverview[len(filesFromOverview)-1].Path
	}

	filesNotInOverview, err := ss.listFiles(&topicName, mostRecentOverviewFile, func(obj types.Object) bool {
		return strings.HasSuffix(*obj.Key, extension)
	})
	if err != nil {
		return nil, err
	}

	totalFiles := len(filesFromOverview) + len(filesNotInOverview)
	log.Debugf("found %d files (%d from overview, %d outside) (%s)", totalFiles, len(filesFromOverview), len(filesNotInOverview), time.Since(t0))

	return append(filesFromOverview, filesNotInOverview...), nil
}

// listFilesFromOverview finds the most recent overview file and returns the
// Files it contains.
// NOTE: the overview file must be in JSON format and gzipped.
func (ss *S3Storage) listFilesFromOverview(topicPrefix string) ([]File, error) {
	overviewPrefix := path.Join(topicPrefix, "overview")
	overviewFiles, err := ss.listFiles(&overviewPrefix, nil, func(obj types.Object) bool {
		return strings.HasSuffix(*obj.Key, ".json.gz")
	})
	if err != nil {
		return nil, err
	}

	if len(overviewFiles) == 0 {
		return nil, nil
	}

	overviewFilePath := overviewFiles[len(overviewFiles)-1].Path
	obj, err := ss.s3.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(ss.bucketName),
		Key:    aws.String(overviewFilePath),
	})
	if err != nil {
		return nil, fmt.Errorf("reading overview file '%s': %w", overviewFilePath, err)
	}
	defer obj.Body.Close()

	rdr, err := gzip.NewReader(obj.Body)
	if err != nil {
		return nil, fmt.Errorf("creating gzip reader: %w", err)
	}

	files := []File{}
	err = json.NewDecoder(rdr).Decode(&files)
	if err != nil {
		return nil, fmt.Errorf("parsing overview file '%s': %w", overviewFilePath, err)
	}

	return files, nil
}

// listFiles is a small wrapper around S3's ListObjectsV2
func (ss *S3Storage) listFiles(prefix *string, startAfter *string, keep func(types.Object) bool) ([]File, error) {
	paginator := s3.NewListObjectsV2Paginator(ss.s3, &s3.ListObjectsV2Input{
		Bucket:     aws.String(ss.bucketName),
		Prefix:     prefix,
		StartAfter: startAfter,
	})

	files := []File{}
	for paginator.HasMorePages() {
		result, err := paginator.NextPage(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("retrieving overview pages: %w", err)
		}

		for _, obj := range result.Contents {
			if obj.Key == nil {
				continue
			}

			if keep(obj) {
				files = append(files, File{
					Path: *obj.Key,
					Size: *obj.Size,
				})
			}
		}
	}

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
