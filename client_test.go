package seb_test

import (
	"testing"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/micvbang/simple-event-broker/internal/tester"
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
	require.ErrorIs(t, err, storage.ErrOutOfBounds)

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
