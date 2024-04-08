package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/micvbang/go-helpy/filey"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

type S3StorageInput struct {
	S3             s3iface.S3API
	LocalCacheRoot *string
	BucketName     string
	RootDir        string
	TopicName      string
}

type S3TopicStorage struct {
	log            logger.Logger
	s3             s3iface.S3API
	localCacheRoot string
	bucketName     string
}

func NewS3TopicStorage(log logger.Logger, input S3StorageInput) (*TopicStorage, error) {
	localCacheRoot := input.RootDir
	if input.LocalCacheRoot != nil {
		localCacheRoot = *input.LocalCacheRoot
	}

	s3Storage := &S3TopicStorage{
		log:            log,
		s3:             input.S3,
		bucketName:     input.BucketName,
		localCacheRoot: localCacheRoot,
	}

	return NewTopicStorage(log, s3Storage, input.RootDir, input.TopicName)
}

func (ss *S3TopicStorage) Writer(recordBatchPath string) (io.WriteCloser, error) {
	cacheRecordBatchPath := ss.recordBatchCachePath(recordBatchPath)
	log := ss.log.
		WithField("cacheRecordBatchPath", cacheRecordBatchPath).
		WithField("recordBatchPath", recordBatchPath)

	log.Debugf("checking cache for record batch")
	if filey.Exists(cacheRecordBatchPath) {
		log.Errorf("Record already exists. This should not happen!")
		return nil, fmt.Errorf("file already exists '%s'", cacheRecordBatchPath)
	}

	log.Debugf("creating temp file")
	tmpFile, err := os.CreateTemp("", "seb_*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	log = log.WithField("temp file", tmpFile.Name())

	log.Debugf("creating s3WriteCloser")

	writeCloser := &s3WriteCloser{f: tmpFile, s3Upload: func(rd io.ReadSeeker) error {
		log.Infof("uploading to s3")
		_, err := ss.s3.PutObject(&s3.PutObjectInput{
			Bucket: &ss.bucketName,
			Key:    &recordBatchPath,
			Body:   rd,
		})
		if err != nil {
			return err
		}

		// NOTE: we don't _need_ the temp file to be moved into the cache, so
		// all is good if the following fails.
		// However: IT'S VERY IMPORTANT that we don't add the file to the cache if it
		// wasn't successfully uploaded to s3 since s3 is our source of truth!
		log.Debugf("creating cache dir")
		err = ss.makeCacheDirs(cacheRecordBatchPath)
		if err != nil {
			return nil
		}

		err = tmpFile.Close()
		if err != nil {
			return nil
		}

		log.Debugf("moving temporary file to cache")
		err = os.Rename(tmpFile.Name(), cacheRecordBatchPath)
		if err != nil {
			return nil
		}

		return nil
	}}

	return writeCloser, nil
}

func (ss *S3TopicStorage) Reader(recordBatchPath string) (io.ReadSeekCloser, error) {
	cacheRecordBatchPath := ss.recordBatchCachePath(recordBatchPath)
	log := ss.log.
		WithField("cacheRecordBatchPath", cacheRecordBatchPath).
		WithField("recordBatchPath", recordBatchPath)

	log.Debugf("checking cache for record batch")

	// check if file is already cached
	f, err := os.Open(cacheRecordBatchPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("checking for file in cache '%s': %w", cacheRecordBatchPath, err)
	}
	if f != nil {
		// file in cache, don't fetch from s3
		return f, nil
	}

	log.Debugf("fetching record batch from s3")
	// file not in cache
	obj, err := ss.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(ss.bucketName),
		Key:    &recordBatchPath,
	})
	if err != nil {
		return nil, fmt.Errorf("retrieving s3 object: %w", err)
	}
	defer obj.Body.Close()

	log.Debugf("creating cache file")
	f, err = ss.createCacheFile(cacheRecordBatchPath)
	if err != nil {
		return nil, err
	}

	log.Debugf("copying s3 object to cache file")
	_, err = io.Copy(f, obj.Body)
	if err != nil {
		return nil, fmt.Errorf("writing s3 object to disk '%s': %w", cacheRecordBatchPath, err)
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking to beginning of file: %w", err)
	}

	return f, nil
}

func (ss *S3TopicStorage) ListFiles(topicPath string, extension string) ([]File, error) {
	log := ss.log.
		WithField("topicPath", topicPath).
		WithField("extension", extension)

	files := make([]File, 0, 128)

	topicPath, _ = strings.CutPrefix(topicPath, "/")

	log.Debugf("listing objects in s3")
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

	log.Debugf("found %d files", len(files))

	return files, err
}

func (ss *S3TopicStorage) recordBatchCachePath(recordBatchPath string) string {
	return filepath.Join(ss.localCacheRoot, recordBatchPath)
}

func (ss *S3TopicStorage) createCacheFile(cacheRecordBatchPath string) (*os.File, error) {
	ss.log.Debugf("creating cache dir")
	err := ss.makeCacheDirs(cacheRecordBatchPath)
	if err != nil {
		return nil, err
	}

	ss.log.Debugf("creating cache file")
	f, err := os.Create(cacheRecordBatchPath)
	if err != nil {
		return nil, fmt.Errorf("creating cache record batch '%s': %w", cacheRecordBatchPath, err)
	}

	return f, err
}

func (ss *S3TopicStorage) makeCacheDirs(cacheRecordBatchPath string) error {
	ss.log.Debugf("creating cache dirs")
	err := os.MkdirAll(filepath.Dir(cacheRecordBatchPath), os.ModePerm)
	if err != nil {
		return fmt.Errorf("creating cache topic dir: %w", err)
	}

	return nil
}

type s3WriteCloser struct {
	f        *os.File
	s3Upload func(io.ReadSeeker) error
}

func (swc *s3WriteCloser) Write(b []byte) (int, error) {
	return swc.f.Write(b)
}

func (swc *s3WriteCloser) Close() error {
	_, err := swc.f.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seeking to beginning: %w", err)
	}

	err = swc.s3Upload(swc.f)
	if err != nil {
		return fmt.Errorf("uploading to s3: %w", err)
	}

	// NOTE: swc.s3Upload() is responsible for closing swc.f.

	return nil
}
