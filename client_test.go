package seb_test

import (
	"context"
	"testing"
	"time"

	"github.com/micvbang/go-helpy/slicey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"

	"github.com/stretchr/testify/require"
)

func TestRecordClientAddRecordsHappyPath(t *testing.T) {
	srv := tester.HTTPServer(t)
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	const (
		topicName = "topicName"
		offset    = 0
	)

	// ensure record does not already exist
	_, err = srv.Broker.GetRecord(topicName, offset)
	require.ErrorIs(t, err, seb.ErrOutOfBounds)

	expectedBatch := tester.MakeRandomRecordBatch(5)
	expectedRecords := tester.BatchIndividualRecords(t, expectedBatch, 0, expectedBatch.Len())

	// Act
	err = client.AddRecords(topicName, expectedBatch.Sizes(), expectedBatch.Data())
	require.NoError(t, err)

	// Assert
	gotRecords, err := srv.Broker.GetRecords(context.Background(), topicName, offset, 100, 0)
	require.NoError(t, err)

	require.Equal(t, expectedRecords, gotRecords)
}

// TestRecordClientAddRecordsNotAuthorized verifies that ErrNotAuthorized is
// returned when using an invalid API key.
func TestRecordClientAddRecordsNotAuthorized(t *testing.T) {
	srv := tester.HTTPServer(t, tester.HTTPAPIKey("working-api-key"))
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, "invalid-api-key")
	require.NoError(t, err)

	// Act
	err = client.AddRecords("topicName", []uint32{}, []byte{})
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
	batch := tester.MakeRandomRecordBatch(1)

	err = client.AddRecords(topicName, batch.Sizes(), batch.Data())
	require.NoError(t, err)

	// Act
	gotRecord, err := client.GetRecord(topicName, offset)
	require.NoError(t, err)

	// Assert
	require.Equal(t, batch.Data(), gotRecord)
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

	batch := tester.MakeRandomRecordBatch(16)
	expectedRecords := tester.BatchIndividualRecords(t, batch, 0, batch.Len())
	_, err := srv.Broker.AddRecords(topicName, batch)
	require.NoError(t, err)

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
	srv := tester.HTTPServer(t, tester.HTTPBrokerAutoCreateTopic(false))
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

	offsets, err := srv.Broker.AddRecords(topicName, tester.MakeRandomRecordBatch(1))
	require.NoError(t, err)

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	offsetTooHigh := slicey.Last(offsets) + 1

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
	deps := &httphandlers.MockDependencies{}
	deps.AddRecordsMock = func(topicName string, batch sebrecords.Batch) ([]uint64, error) {
		return nil, seb.ErrPayloadTooLarge
	}

	srv := tester.HTTPServer(t, tester.HTTPDependencies(deps))
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	// Act
	err = client.AddRecords("topicName", []uint32{}, []byte{})

	// Assert
	require.ErrorIs(t, err, seb.ErrPayloadTooLarge)
}
