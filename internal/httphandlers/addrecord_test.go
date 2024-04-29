package httphandlers_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

// TestAddRecordHappyPath verifies that http.StatusCreated is returned for a
// valid request to POST /record.
func TestAddRecordHappyPath(t *testing.T) {
	const topicName = "topic"

	// add record s.t. returned record id in HTTP response is not 0 (default value)
	server := tester.HTTPServer(t)
	_, err := server.Storage.AddRecord(topicName, recordbatch.Record("haps"))
	require.NoError(t, err)

	r := httptest.NewRequest("POST", "/record", bytes.NewReader(nil))
	httphelpers.AddQueryParams(r, map[string]string{
		"topic-name": topicName,
	})

	response := server.DoWithAuth(r)
	require.Equal(t, http.StatusCreated, response.StatusCode)
	require.Equal(t, "application/json", response.Header.Get("Content-Type"))

	output := httphandlers.AddRecordOutput{}
	err = httphelpers.ParseJSONAndClose(response.Body, &output)
	require.NoError(t, err)

	require.Equal(t, uint64(1), output.RecordID)
}

// Verifies that http.BadRequest is returned when leaving out the required
// topic-name query parameter.
func TestAddRecordMissingTopic(t *testing.T) {
	r := httptest.NewRequest("POST", "/record", bytes.NewReader(nil))
	httphelpers.AddQueryParams(r, map[string]string{
		// NOTE: no topic-name set
	})

	response := tester.HTTPServer(t).DoWithAuth(r)
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
}
