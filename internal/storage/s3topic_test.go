package storage

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/micvbang/go-helpy/stringy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

var log = logger.NewDefault(context.Background())

// TestS3WriteToS3 verifies that Writer creates an io.WriteCloser that calls
// s3's PutObject method with the given data once the io.WriteCloser is closed.
func TestS3WriteToS3(t *testing.T) {
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

	s3Storage := &S3TopicStorage{
		log:        log,
		s3:         s3Mock,
		bucketName: bucketName,
	}

	// Act
	rbWriter, err := s3Storage.Writer(recordBatchPath)
	require.NoError(t, err)

	n, err := rbWriter.Write(randomBytes)
	require.NoError(t, err)
	require.Equal(t, len(randomBytes), n)

	// Assert
	// file should not be written to s3 before it's closed
	require.False(t, s3Mock.PutObjectCalled)

	// file should be written to s3 when it's closed
	err = rbWriter.Close()
	require.NoError(t, err)
	require.True(t, s3Mock.PutObjectCalled)
}

// TestS3ReadFromS3 verifies that Reader returns an io.Reader that returns
// the bytes that were fetched from S3.
func TestS3ReadFromS3(t *testing.T) {
	recordBatchPath := "topicName/000123.record_batch"
	expectedBytes := []byte(stringy.RandomN(500))

	s3Mock := &tester.S3Mock{}
	s3Mock.MockGetObject = func(goi *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewBuffer(expectedBytes)),
		}, nil
	}

	s3Storage := &S3TopicStorage{
		log:        log,
		s3:         s3Mock,
		bucketName: "mybucket",
	}

	// Act
	rdr, err := s3Storage.Reader(recordBatchPath)
	require.NoError(t, err)
	defer rdr.Close()

	// Assert
	gotBytes, err := io.ReadAll(rdr)
	require.NoError(t, err)

	require.Equal(t, expectedBytes, gotBytes)
}

// TestListFiles verifies that ListFiles returns a list of the files outputted
// by s3's ListObjectsPages's successive calls to the provided callback.
func TestListFiles(t *testing.T) {
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

	s3Storage := &S3TopicStorage{
		log:        log,
		s3:         s3Mock,
		bucketName: "mybucket",
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
