package seb_test

import (
	"testing"
	"time"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/stretchr/testify/require"
)

// TestRecordClientAddHappyPath verifies that Add makes a valid HTTP POST to the
// endpoint for adding a record.
func TestRecordClientAddHappyPath(t *testing.T) {
	srv := tester.HTTPServer(t)
	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	const (
		topicName = "topicName"
		offset    = 0
	)
	expectedRecord := recordbatch.Record("this is my record!")

	// record does not already exist
	_, err = srv.Storage.GetRecord(topicName, offset)
	require.ErrorIs(t, err, seb.ErrOutOfBounds)

	// Act
	err = client.Add(topicName, expectedRecord)
	require.NoError(t, err)

	// Assert
	gotRecord, err := srv.Storage.GetRecord(topicName, offset)
	require.NoError(t, err)
	require.Equal(t, expectedRecord, gotRecord)
}

// TestRecordClientAddNotAuthorized verifies that ErrNotAuthorized is returned
// when using an invalid API key.
func TestRecordClientAddNotAuthorized(t *testing.T) {
	srv := tester.HTTPServer(t, tester.HTTPAPIKey("working-api-key"))
	client, err := seb.NewRecordClient(srv.Server.URL, "invalid-api-key")
	require.NoError(t, err)

	// Act
	err = client.Add("topicName", recordbatch.Record("this is my record!"))
	require.ErrorIs(t, err, seb.ErrNotAuthorized)
}

// TestRecordClientGetHappyPath verifies that Get makes a valid HTTP GET to the
// endpoint for getting a record.
func TestRecordClientGetHappyPath(t *testing.T) {
	srv := tester.HTTPServer(t)
	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	const (
		topicName = "topicName"
		offset    = 0
	)
	expectedRecord := recordbatch.Record("this is my record!")

	err = client.Add(topicName, expectedRecord)
	require.NoError(t, err)

	// Act
	gotRecord, err := client.Get(topicName, offset)
	require.NoError(t, err)

	// Assert
	require.Equal(t, expectedRecord, gotRecord)
}

// TestRecordClientGetNotAuthorized verifies that Get returns ErrNotAuthorized when
// using an invalid API key.
func TestRecordClientGetNotAuthorized(t *testing.T) {
	srv := tester.HTTPServer(t, tester.HTTPAPIKey("working-api-key"))
	client, err := seb.NewRecordClient(srv.Server.URL, "invalid-api-key")
	require.NoError(t, err)

	// Act
	_, err = client.Get("topicName", 0)

	// Assert
	require.ErrorIs(t, err, seb.ErrNotAuthorized)
}

// TestRecordClientGetNotFound verifies that Get returns ErrNotFound when
// attempting to retrieve a record with an offset that does not exist.
func TestRecordClientGetNotFound(t *testing.T) {
	srv := tester.HTTPServer(t)
	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	// Act
	_, err = client.Get("topicName", 0)

	// Assert
	require.ErrorIs(t, err, seb.ErrNotFound)
}

// TestRecordClientGetBatchHappyPath verifies that GetBatch returns the expected
// records when everything goes well.
func TestRecordClientGetBatchHappyPath(t *testing.T) {
	const topicName = "topic-name"
	srv := tester.HTTPServer(t)

	expectedRecords := make(recordbatch.RecordBatch, 16)
	for i := range len(expectedRecords) {
		expectedRecords[i] = tester.RandomBytes(t, 32)
		_, err := srv.Storage.AddRecord(topicName, expectedRecords[i])
		require.NoError(t, err)
	}

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	// Act
	gotRecords, err := client.GetBatch(topicName, 0, seb.GetBatchInput{
		MaxRecords:   len(expectedRecords),
		SoftMaxBytes: 9999999,
		Timeout:      1 * time.Minute,
	})
	require.NoError(t, err)

	// Assert
	require.Equal(t, expectedRecords, gotRecords)
}

// TestRecordClientGetBatchTopicDoesNotExist verifies that ErrNotFound is
// returned when attempting to read from a topic that does not exist.
func TestRecordClientGetBatchTopicDoesNotExist(t *testing.T) {
	srv := tester.HTTPServer(t)

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	offset := uint64(0)

	// Act
	_, err = client.GetBatch("does-not-exist", offset, seb.GetBatchInput{})

	// Assert
	// TODO: we would like to distinguish between "record not found" and "topic not found".
	require.ErrorIs(t, err, seb.ErrNotFound)
}

// TestRecordClientGetBatchTopicDoesNotExist verifies that ErrNotFound is
// returned when attempting to read from an offset that does not exist.
func TestRecordClientGetBatchOffsetOutOfBounds(t *testing.T) {
	const topicName = "topic-name"
	srv := tester.HTTPServer(t)

	offset, err := srv.Storage.AddRecord(topicName, recordbatch.Record("this be record"))
	require.NoError(t, err)

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	offsetTooHigh := offset + 1

	// Act
	_, err = client.GetBatch(topicName, offsetTooHigh, seb.GetBatchInput{})

	// Assert
	// TODO: we would like to distinguish between "record not found" and "topic not found".
	require.ErrorIs(t, err, seb.ErrNotFound)
}
