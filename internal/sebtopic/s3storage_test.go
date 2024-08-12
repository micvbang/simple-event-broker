package sebtopic_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/micvbang/go-helpy/stringy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/micvbang/simple-event-broker/seberr"
	"github.com/stretchr/testify/require"
)

// TestS3WriteToS3 verifies that Writer creates an io.WriteCloser that calls
// S3's PutObject method with the given data once the io.WriteCloser is closed.
func TestS3WriteToS3(t *testing.T) {
	bucketName := "mybucket"
	recordBatchPath := "topicName/000123.record_batch"
	randomBytes := []byte(stringy.RandomN(500))

	s3Mock := &tester.S3Mock{}
	s3Mock.MockPutObject = func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
		// Verify the expected parameters are passed on to S3
		require.Equal(t, *params.Bucket, bucketName)
		require.Equal(t, *params.Key, recordBatchPath)

		gotBody, err := io.ReadAll(params.Body)
		require.NoError(t, err)
		require.EqualValues(t, randomBytes, gotBody)

		return nil, nil
	}

	s3Storage := sebtopic.NewS3Storage(log, s3Mock, bucketName, "")

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

// TestS3WriteToS3WithError verifies that an error is returned when uploading to
// S3 fails.
func TestS3WriteToS3WithError(t *testing.T) {
	bucketName := "mybucket"
	recordBatchPath := "topicName/000123.record_batch"
	randomBytes := []byte(stringy.RandomN(500))

	expectedErr := fmt.Errorf("PutObject failed")
	s3Mock := &tester.S3Mock{}
	s3Mock.MockPutObject = func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
		return nil, expectedErr
	}

	s3Storage := sebtopic.NewS3Storage(log, s3Mock, bucketName, "")

	// Act
	rbWriter, err := s3Storage.Writer(recordBatchPath)
	require.NoError(t, err)

	n, err := rbWriter.Write(randomBytes)
	require.NoError(t, err)
	require.Equal(t, len(randomBytes), n)

	// file should be written to s3 when it's closed
	err = rbWriter.Close()
	require.ErrorIs(t, err, expectedErr)
	require.True(t, s3Mock.PutObjectCalled)
}

// TestS3WriteWithPrefix verifies that the given prefix is used when calling
// S3's PutObject.
func TestS3WriteWithPrefix(t *testing.T) {
	expectedBytes := tester.RandomBytes(t, 512)

	const (
		s3KeyPrefix     = "some-prefix"
		recordBatchPath = "topicName/000123.record_batch"
	)
	expectedKey := path.Join(s3KeyPrefix, recordBatchPath)

	s3Mock := &tester.S3Mock{}
	s3Mock.MockPutObject = func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
		// Verify the expected parameters are passed on to S3
		require.Equal(t, expectedKey, *params.Key)

		return nil, nil
	}

	s3Storage := sebtopic.NewS3Storage(log, s3Mock, "mybucket", "some-prefix")

	// Act
	wtr, err := s3Storage.Writer(recordBatchPath)
	require.NoError(t, err)
	tester.WriteAndClose(t, wtr, expectedBytes)
	require.True(t, s3Mock.PutObjectCalled)
}

// TestS3ReadFromS3 verifies that Reader returns an io.Reader that returns
// calls S3's GetObject method, returning the bytes that were fetched from S3.
func TestS3ReadFromS3(t *testing.T) {
	recordBatchPath := "topicName/000123.record_batch"
	expectedBytes := tester.RandomBytes(t, 512)

	s3Mock := &tester.S3Mock{}
	s3Mock.MockGetObject = func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewBuffer(expectedBytes)),
		}, nil
	}

	s3Storage := sebtopic.NewS3Storage(log, s3Mock, "mybucket", "")

	// Act
	rdr, err := s3Storage.Reader(recordBatchPath)
	require.NoError(t, err)
	defer rdr.Close()

	// Assert
	gotBytes, err := io.ReadAll(rdr)
	require.NoError(t, err)

	require.Equal(t, expectedBytes, gotBytes)
}

// TestS3ReadWithPrefix verifies that the given prefix is used when calling S3's
// GetObject.
func TestS3ReadWithPrefix(t *testing.T) {
	expectedBytes := tester.RandomBytes(t, 512)

	const (
		s3KeyPrefix     = "some-prefix"
		recordBatchPath = "topicName/000123.record_batch"
	)
	expectedPath := path.Join(s3KeyPrefix, recordBatchPath)

	s3Mock := &tester.S3Mock{}
	s3Mock.MockGetObject = func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
		// Verify the expected parameters are passed on to S3
		require.Equal(t, expectedPath, *params.Key)
		return &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewBuffer(expectedBytes)),
		}, nil
	}

	s3Storage := sebtopic.NewS3Storage(log, s3Mock, "mybucket", "some-prefix")

	// Act
	rdr, err := s3Storage.Reader(recordBatchPath)
	require.NoError(t, err)

	tester.ReadAndClose(t, rdr)
	require.True(t, s3Mock.GetObjectCalled)
}

// TestListFiles verifies that ListFiles returns a list of the files outputted
// by S3's ListObjectsPages's successive calls to the provided callback.
func TestListFiles(t *testing.T) {
	listObjectOutputBatches := [][]sebtopic.File{
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

	expectedFiles := []sebtopic.File{}
	for _, batch := range listObjectOutputBatches {
		expectedFiles = append(expectedFiles, batch...)
	}

	batchI := 0
	s3Mock := &tester.S3Mock{}
	s3Mock.MockListObjectsV2 = func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
		output := listObjectsOutputFromFiles(listObjectOutputBatches[batchI])
		batchI += 1
		lastPage := batchI == len(listObjectOutputBatches)
		if !lastPage {
			output.IsTruncated = aws.Bool(true)
			output.NextContinuationToken = aws.String("more data!")
		}
		return output, nil
	}

	s3Storage := sebtopic.NewS3Storage(log, s3Mock, "mybucket", "")

	gotFiles, err := s3Storage.ListFiles("dummy/dir", ".ext")
	require.NoError(t, err)

	require.Equal(t, expectedFiles, gotFiles)
}

// TestListFilesOverlappingNames verifies that ListFiles formats Prefix in
// requests to S3 ListObjectPages correctly, i.e. removes any prefix "/", and
// ensures that it has "/" as a suffix.
func TestListFilesOverlappingNames(t *testing.T) {
	s3Mock := &tester.S3Mock{}
	s3Mock.MockListObjectsV2 = func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
		// Assert
		require.False(t, strings.HasPrefix(*params.Prefix, "/"))
		require.True(t, strings.HasSuffix(*params.Prefix, "/"))
		return &s3.ListObjectsV2Output{}, nil
	}

	s3Storage := sebtopic.NewS3Storage(log, s3Mock, "mybucket", "")

	// Act
	testPrefixes := []string{
		"dummy/dir",
		"/dummy/dir",
		"dummy/dir/",
		"/dummy/dir/",
	}

	for _, prefix := range testPrefixes {
		t.Run(fmt.Sprintf("prefix '%s'", prefix), func(t *testing.T) {
			_, err := s3Storage.ListFiles(prefix, ".ext")
			require.NoError(t, err)
		})
	}

	// Assert that MockListObjectPages has been called (and its assertions have
	// been run)
	require.True(t, s3Mock.ListObjectPagesCalled)
}

func listObjectsOutputFromFiles(files []sebtopic.File) *s3.ListObjectsV2Output {
	s3Objects := make([]types.Object, len(files))

	for i := range files {
		s3Objects[i] = types.Object{
			Key:  &files[i].Path,
			Size: &files[i].Size,
		}
	}

	return &s3.ListObjectsV2Output{
		Contents: s3Objects,
	}
}

func TestS3ReadFromS3NotFound(t *testing.T) {
	recordBatchPath := "topicName/000123.record_batch"

	s3Mock := &tester.S3Mock{}
	s3Mock.MockGetObject = func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey"}
	}

	s3Storage := sebtopic.NewS3Storage(log, s3Mock, "mybucket", "")

	// Act
	_, err := s3Storage.Reader(recordBatchPath)

	// Assert
	require.ErrorIs(t, err, seberr.ErrNotInStorage)
}
