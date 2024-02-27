package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/micvbang/go-helpy/filey"
	"github.com/micvbang/go-helpy/stringy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

var log = logger.NewDefault(context.Background())

// TestS3WriteToS3 verifies that Writer creates an io.WriteCloser that calls
// s3's PutObject method with the given data once the io.WriteCloser is closed.
func TestS3WriteToS3(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	bucketName := "mybucket"
	recordBatchPath := "topicName/000123.record_batch"
	randomBytes := []byte(stringy.RandomN(500))

	s3Mock := &tester.S3Mock{}
	s3Mock.MockPutObject = func(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
		// Verify the expected parameters are passed on to S3
		require.Equal(t, *input.Bucket, bucketName)
		require.Equal(t, *input.Key, recordBatchPath)

		gotBody, err := io.ReadAll(input.Body)
		require.NoError(t, err)
		require.EqualValues(t, randomBytes, gotBody)

		return nil, nil
	}

	s3Storage := &S3Storage{
		log:            log,
		s3:             s3Mock,
		localCacheRoot: tempDir,
		bucketName:     bucketName,
	}

	// Test
	rbWriter, err := s3Storage.Writer(recordBatchPath)
	require.NoError(t, err)

	n, err := rbWriter.Write(randomBytes)
	require.NoError(t, err)
	require.Equal(t, len(randomBytes), n)

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
	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	recordBatchPath := "topicName/000123.record_batch"
	expectedBytes := []byte(stringy.RandomN(500))

	s3Mock := &tester.S3Mock{}
	s3Mock.MockPutObject = func(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
		return nil, nil
	}

	s3Storage := &S3Storage{
		log:            log,
		s3:             s3Mock,
		localCacheRoot: tempDir,
		bucketName:     "mybucket",
	}

	// Test
	rbWriter, err := s3Storage.Writer(recordBatchPath)
	require.NoError(t, err)

	n, err := rbWriter.Write(expectedBytes)
	require.NoError(t, err)
	require.Equal(t, len(expectedBytes), n)

	err = rbWriter.Close()
	require.NoError(t, err)

	// Verify
	rd, err := s3Storage.Reader(recordBatchPath)
	require.NoError(t, err)

	gotBytes, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, expectedBytes, gotBytes)
}

// TestS3ReadFromCache verifies that Reader returns an io.Reader that returns
// the bytes that were fetched from S3.
func TestS3ReadFromCache(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	recordBatchPath := "topicName/000123.record_batch"
	expectedBytes := []byte(stringy.RandomN(500))

	s3Mock := &tester.S3Mock{}
	s3Mock.MockGetObject = func(goi *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewBuffer(expectedBytes)),
		}, nil
	}

	s3Storage := &S3Storage{
		log:            log,
		s3:             s3Mock,
		localCacheRoot: tempDir,
		bucketName:     "mybucket",
	}

	rdr, err := s3Storage.Reader(recordBatchPath)
	require.NoError(t, err)
	defer rdr.Close()

	gotBytes, err := io.ReadAll(rdr)
	require.NoError(t, err)

	require.Equal(t, expectedBytes, gotBytes)
}

// TestWrittenToS3BeforeCacheIsPopulated verifies that the payload has reached
// S3 before it's written to the cache.
// This is super important since S3 is our source of truth. If we write to the
// cache before uploading to S3 and the server dies before our S3 upload is
// complete, we could be in the situation where we served a request to retrieve
// the data that only exists in the local cache but not in S3. This data is now
// lost, and subsequent requests for the same id will not return the same data.
func TestWrittenToS3BeforeCacheIsPopulated(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	recordBatchPath := "topicName/000123.record_batch"
	randomBytes := []byte(stringy.RandomN(500))

	putReturn := make(chan struct{})
	s3Mock := &tester.S3Mock{}
	s3Mock.MockPutObject = func(poi *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
		// block until putReturn is closed
		<-putReturn
		return nil, nil
	}

	s3Storage := &S3Storage{
		log:            log,
		s3:             s3Mock,
		localCacheRoot: tempDir,
		bucketName:     "mybucket",
	}

	wtr, err := s3Storage.Writer(recordBatchPath)
	require.NoError(t, err)

	n, err := wtr.Write(randomBytes)
	require.NoError(t, err)
	require.Equal(t, len(randomBytes), n)

	expectedCachePath := filepath.Join(tempDir, recordBatchPath)
	require.False(t, filey.Exists(expectedCachePath))

	closeReturn := make(chan struct{})
	go func() {
		err := wtr.Close()
		require.NoError(t, err)
		close(closeReturn)
	}()

	// simulate PutObject taking some time to finish
	time.Sleep(250 * time.Millisecond)
	require.False(t, filey.Exists(expectedCachePath))

	close(putReturn) // make PutObject return
	<-closeReturn    // wait for wtr.Close() to write file to cache and return
	require.True(t, filey.Exists(expectedCachePath))
}

// TestListFiles verifies that ListFiles returns a list of the files outputted
// by s3's ListObjectsPages's successive calls to the provided callback.
func TestListFiles(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	listObjectOutputBatches := [][]File{
		{
			{Path: "dummy1/name1.ext", Size: 101},
			{Path: "dummy1/name2.ext", Size: 102},
			{Path: "dummy1/name3.ext", Size: 103},
		},
		{
			{Path: "dummy2/name1.ext", Size: 201},
			{Path: "dummy2/name2.ext", Size: 202},
			{Path: "dummy2/name3.ext", Size: 203},
		},
		{
			{Path: "dummy3/name1.ext", Size: 301},
		},
	}

	expectedFiles := []File{}
	for _, batch := range listObjectOutputBatches {
		expectedFiles = append(expectedFiles, batch...)
	}

	s3Mock := &tester.S3Mock{}
	s3Mock.MockListObjectPages = func(input *s3.ListObjectsInput, f func(*s3.ListObjectsOutput, bool) bool) error {
		for i, listObjectBatch := range listObjectOutputBatches {
			listObjectsOutput := listObjectsOutputFromFiles(listObjectBatch)
			lastPage := i == len(listObjectOutputBatches)-1

			more := f(listObjectsOutput, lastPage)
			if !more {
				break
			}
		}

		return nil
	}

	s3Storage := &S3Storage{
		log:            log,
		s3:             s3Mock,
		localCacheRoot: tempDir,
		bucketName:     "mybucket",
	}

	gotFiles, err := s3Storage.ListFiles("dummy/dir", ".ext")
	require.NoError(t, err)

	require.Equal(t, expectedFiles, gotFiles)
}

func listObjectsOutputFromFiles(files []File) *s3.ListObjectsOutput {
	s3Objects := make([]*s3.Object, len(files))

	for i := range files {
		s3Objects[i] = &s3.Object{
			Key:  &files[i].Path,
			Size: &files[i].Size,
		}
	}

	return &s3.ListObjectsOutput{
		Contents: s3Objects,
	}
}
