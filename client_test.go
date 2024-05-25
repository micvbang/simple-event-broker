package seb_test

import (
	"context"
	"testing"
	"time"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/sebhttp"
	"github.com/stretchr/testify/require"
)

// TestRecordClientAddRecordHappyPath verifies that Add makes a valid HTTP POST
// to the endpoint for adding a record.
func TestRecordClientAddRecordHappyPath(t *testing.T) {
	srv := tester.HTTPServer(t)
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	const (
		topicName = "topicName"
		offset    = 0
	)
	expectedRecord := []byte("this is my record!")

	// record does not already exist
	_, err = srv.Storage.GetRecord(topicName, offset)
	require.ErrorIs(t, err, seb.ErrOutOfBounds)

	// Act
	err = client.AddRecord(topicName, expectedRecord)
	require.NoError(t, err)

	// Assert
	gotRecord, err := srv.Storage.GetRecord(topicName, offset)
	require.NoError(t, err)
	require.Equal(t, expectedRecord, []byte(gotRecord))
}

// TestRecordClientAddRecordNotAuthorized verifies that ErrNotAuthorized is
// returned when using an invalid API key.
func TestRecordClientAddRecordNotAuthorized(t *testing.T) {
	srv := tester.HTTPServer(t, tester.HTTPAPIKey("working-api-key"))
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, "invalid-api-key")
	require.NoError(t, err)

	// Act
	err = client.AddRecord("topicName", []byte("this is my record!"))
	require.ErrorIs(t, err, seb.ErrNotAuthorized)
}

func TestRecordClientAddRecordsHappyPath(t *testing.T) {
	srv := tester.HTTPServer(t)
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	const (
		topicName = "topicName"
		offset    = 0
	)
	expectedRecords := [][]byte{
		tester.RandomBytes(t, 32),
		tester.RandomBytes(t, 32),
		tester.RandomBytes(t, 32),
		tester.RandomBytes(t, 32),
		tester.RandomBytes(t, 32),
	}

	// ensure record does not already exist
	_, err = srv.Storage.GetRecord(topicName, offset)
	require.ErrorIs(t, err, seb.ErrOutOfBounds)

	// Act
	err = client.AddRecords(topicName, expectedRecords)
	require.NoError(t, err)

	// Assert
	gotRecords, err := srv.Storage.GetRecords(context.Background(), topicName, offset, 100, 0)
	require.NoError(t, err)
	gotRecordsBytes := make([][]byte, len(gotRecords))
	for i, record := range gotRecords {
		gotRecordsBytes[i] = record
	}
	require.Equal(t, expectedRecords, gotRecordsBytes)
}

// TestRecordClientAddRecordsNotAuthorized verifies that ErrNotAuthorized is
// returned when using an invalid API key.
func TestRecordClientAddRecordsNotAuthorized(t *testing.T) {
	srv := tester.HTTPServer(t, tester.HTTPAPIKey("working-api-key"))
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, "invalid-api-key")
	require.NoError(t, err)

	// Act
	err = client.AddRecords("topicName", [][]byte{tester.RandomBytes(t, 8)})
	require.ErrorIs(t, err, seb.ErrNotAuthorized)
}

// TestRecordClientGetRecordHappyPath verifies that Get makes a valid HTTP GET
// to the endpoint for getting a record.
func TestRecordClientGetRecordHappyPath(t *testing.T) {
	srv := tester.HTTPServer(t)
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	const (
		topicName = "topicName"
		offset    = 0
	)
	expectedRecord := []byte("this is my record!")

	err = client.AddRecord(topicName, expectedRecord)
	require.NoError(t, err)

	// Act
	gotRecord, err := client.GetRecord(topicName, offset)
	require.NoError(t, err)

	// Assert
	require.Equal(t, expectedRecord, gotRecord)
}

// TestRecordClientGetRecordNotAuthorized verifies that Get returns
// ErrNotAuthorized when using an invalid API key.
func TestRecordClientGetRecordNotAuthorized(t *testing.T) {
	srv := tester.HTTPServer(t, tester.HTTPAPIKey("working-api-key"))
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, "invalid-api-key")
	require.NoError(t, err)

	// Act
	_, err = client.GetRecord("topicName", 0)

	// Assert
	require.ErrorIs(t, err, seb.ErrNotAuthorized)
}

// TestRecordClientGetRecordNotFound verifies that Get returns ErrNotFound when
// attempting to retrieve a record with an offset that does not exist.
func TestRecordClientGetRecordNotFound(t *testing.T) {
	srv := tester.HTTPServer(t)
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	// Act
	_, err = client.GetRecord("topicName", 0)

	// Assert
	require.ErrorIs(t, err, seb.ErrNotFound)
}

// TestRecordClientGetRecordsHappyPath verifies that GetBatch returns the
// expected records when everything goes well.
func TestRecordClientGetRecordsHappyPath(t *testing.T) {
	const topicName = "topic-name"
	srv := tester.HTTPServer(t)
	defer srv.Close()

	expectedRecords := make([][]byte, 16)
	for i := range len(expectedRecords) {
		expectedRecords[i] = tester.RandomBytes(t, 32)
		_, err := srv.Storage.AddRecord(topicName, expectedRecords[i])
		require.NoError(t, err)
	}

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	// Act
	gotRecords, err := client.GetRecords(topicName, 0, seb.GetRecordsInput{
		MaxRecords:   len(expectedRecords),
		SoftMaxBytes: 9999999,
		Timeout:      1 * time.Minute,
	})
	require.NoError(t, err)

	// Assert
	require.Equal(t, expectedRecords, gotRecords)
}

// TestRecordClientGetRecordsTopicDoesNotExist verifies that ErrNotFound is
// returned when attempting to read from a topic that does not exist.
func TestRecordClientGetRecordsTopicDoesNotExist(t *testing.T) {
	srv := tester.HTTPServer(t, tester.HTTPStorageAutoCreateTopic(false))
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	offset := uint64(0)

	// Act
	_, err = client.GetRecords("does-not-exist", offset, seb.GetRecordsInput{})

	// Assert
	// TODO: we would like to distinguish between "record not found" and "topic not found".
	require.ErrorIs(t, err, seb.ErrNotFound)
}

// TestRecordClientGetRecordsOffsetOutOfBounds verifies that no error is
// returned when attempting to read from an offset that does not exist yet.
func TestRecordClientGetRecordsOffsetOutOfBounds(t *testing.T) {
	const topicName = "topic-name"
	srv := tester.HTTPServer(t)
	defer srv.Close()

	offset, err := srv.Storage.AddRecord(topicName, []byte("this be record"))
	require.NoError(t, err)

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	offsetTooHigh := offset + 1

	// Act
	records, err := client.GetRecords(topicName, offsetTooHigh, seb.GetRecordsInput{
		Timeout: time.Millisecond, // NOTE: amount of time to wait for offset to exist
	})

	// Assert
	require.NoError(t, err)
	require.Equal(t, 0, len(records))
}

// TestRecordClientAddRecordsPayloadTooLarge verifies that AddRecords()
// returns ErrPayloadTooLarge when receiving status code
// http.StatusRequestEntityTooLarge.
func TestRecordClientAddRecordsPayloadTooLarge(t *testing.T) {
	deps := &sebhttp.MockDependencies{}
	deps.AddRecordsMock = func(topicName string, records []recordbatch.Record) ([]uint64, error) {
		return nil, seb.ErrPayloadTooLarge
	}

	srv := tester.HTTPServer(t, tester.HTTPDependencies(deps))
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	// Act
	err = client.AddRecords("topicName", [][]byte{})

	// Assert
	require.ErrorIs(t, err, seb.ErrPayloadTooLarge)
}
