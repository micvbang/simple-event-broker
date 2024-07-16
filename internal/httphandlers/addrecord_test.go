package httphandlers_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/stretchr/testify/require"
)

// TestAddRecordHappyPath verifies that http.StatusCreated is returned for a
// valid request to POST /record.
func TestAddRecordHappyPath(t *testing.T) {
	const topicName = "topic"

	server := tester.HTTPServer(t)
	defer server.Close()

	// add record s.t. returned offset in HTTP response is not 0 (default value)
	_, err := server.Broker.AddRecord(topicName, []byte("haps"))
	require.NoError(t, err)

	expectedRecord := tester.RandomBytes(t, 64)
	expectedOffset := uint64(1)

	r := httptest.NewRequest("POST", "/record", bytes.NewReader(expectedRecord))
	httphelpers.AddQueryParams(r, map[string]string{
		"topic-name": topicName,
	})

	// Act
	response := server.DoWithAuth(r)

	// Assert
	require.Equal(t, http.StatusCreated, response.StatusCode)
	require.Equal(t, "application/json", response.Header.Get("Content-Type"))

	output := httphandlers.AddRecordOutput{}
	err = httphelpers.ParseJSONAndClose(response.Body, &output)
	require.NoError(t, err)
	require.Equal(t, expectedOffset, output.Offset)

	gotRecord, err := server.Broker.GetRecord(topicName, expectedOffset)
	require.NoError(t, err)
	require.Equal(t, expectedRecord, gotRecord)
}

// Verifies that http.BadRequest is returned when leaving out the required
// topic-name query parameter.
func TestAddRecordMissingTopic(t *testing.T) {
	r := httptest.NewRequest("POST", "/record", bytes.NewReader(nil))
	httphelpers.AddQueryParams(r, map[string]string{
		// NOTE: no topic-name set
	})

	server := tester.HTTPServer(t)
	defer server.Close()

	response := server.DoWithAuth(r)
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
}
