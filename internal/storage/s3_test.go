package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/micvbang/go-helpy/stringy"
	"github.com/micvbang/simple-message-broker/internal/infrastructure/logger"
	"github.com/stretchr/testify/require"
)

var log = logger.NewDefault(context.Background())

// TestS3WriteToS3 verifies that Writer creates an io.WriteCloser that calls
// s3's PutObject method with the given data once the io.WriteCloser is closed.
func TestS3WriteToS3(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "smb_*")
	require.NoError(t, err)

	bucketName := "mybucket"
	recordBatchPath := "topicName/000123.record_batch"
	recordBatchBody := []byte(stringy.RandomN(500))

	s3Mock := &S3Mock{}
	s3Mock.MockPutObject = func(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
		// Verify the expected parameters are passed on to S3
		require.Equal(t, *input.Bucket, bucketName)
		require.Equal(t, *input.Key, recordBatchPath)

		gotBody, err := io.ReadAll(input.Body)
		require.NoError(t, err)
		require.Equal(t, recordBatchBody, gotBody)

		return nil, nil
	}

	s3Storage := &S3Storage{
		log:            log,
		s3:             s3Mock,
		topicCacheRoot: tempDir,
		bucketName:     bucketName,
	}

	// Test
	rbWriter, err := s3Storage.Writer(recordBatchPath)
	require.NoError(t, err)

	n, err := rbWriter.Write(recordBatchBody)
	require.NoError(t, err)
	require.Equal(t, len(recordBatchBody), n)

	// Verify
	// file should not be written to s3 before it's closed
	require.False(t, s3Mock.PutObjectCalled)

	// file should be written to s3 when it's closed
	err = rbWriter.Close()
	require.NoError(t, err)
	require.True(t, s3Mock.PutObjectCalled)
}

// TestS3WriteToCache verifies that Writer creates an io.WriteCloser that writes
// the data written to it to a file in the given cache directory.
func TestS3WriteToCache(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "smb_*")
	require.NoError(t, err)

	recordBatchPath := "topicName/000123.record_batch"
	recordBatchBody := []byte(stringy.RandomN(500))

	s3Mock := &S3Mock{}
	s3Mock.MockPutObject = func(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
		return nil, nil
	}

	s3Storage := &S3Storage{
		log:            log,
		s3:             s3Mock,
		topicCacheRoot: tempDir,
		bucketName:     "mybucket",
	}

	// Test
	rbWriter, err := s3Storage.Writer(recordBatchPath)
	require.NoError(t, err)

	n, err := rbWriter.Write(recordBatchBody)
	require.NoError(t, err)
	require.Equal(t, len(recordBatchBody), n)

	err = rbWriter.Close()
	require.NoError(t, err)

	// Verify
	rd, err := s3Storage.Reader(recordBatchPath)
	require.NoError(t, err)

	cacheBody, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, recordBatchBody, cacheBody)
}

// TestS3ReadFromCache verifies that Reader returns an io.Reader that returns
// the bytes that were fetched from S3.
func TestS3ReadFromCache(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "smb_*")
	require.NoError(t, err)

	recordBatchPath := "topicName/000123.record_batch"
	recordBatchBody := []byte(stringy.RandomN(500))

	s3Mock := &S3Mock{}
	s3Mock.MockGetObject = func(goi *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewBuffer(recordBatchBody)),
		}, nil
	}

	s3Storage := &S3Storage{
		log:            log,
		s3:             s3Mock,
		topicCacheRoot: tempDir,
		bucketName:     "mybucket",
	}

	rdr, err := s3Storage.Reader(recordBatchPath)
	require.NoError(t, err)
	defer rdr.Close()

	gotBytes, err := io.ReadAll(rdr)
	require.NoError(t, err)

	require.Equal(t, recordBatchBody, gotBytes)
}

type S3Mock struct {
	s3iface.S3API

	MockPutObject   func(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
	PutObjectCalled bool

	MockGetObject   func(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
	GetObjectCalled bool

	MockListObjectPages   func(*s3.ListObjectsInput, func(*s3.ListObjectsOutput, bool) bool) error
	ListObjectPagesCalled bool
}

func (sm *S3Mock) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	sm.PutObjectCalled = true
	return sm.MockPutObject(input)
}

func (sm *S3Mock) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	sm.GetObjectCalled = true
	return sm.MockGetObject(input)
}

func (sm *S3Mock) ListObjectsPages(input *s3.ListObjectsInput, f func(*s3.ListObjectsOutput, bool) bool) error {
	sm.ListObjectPagesCalled = true
	return sm.MockListObjectPages(input, f)
}
