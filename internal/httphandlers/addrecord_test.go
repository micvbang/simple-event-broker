package httphandlers_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

// TestAddRecordHappyPath verifies that http.StatusCreated is returned for a
// valid request to POST /record.
func TestAddRecordHappyPath(t *testing.T) {
	r := httptest.NewRequest("POST", "/record", bytes.NewReader(nil))
	httphelpers.AddQueryParams(r, map[string]string{
		"topic-name": "topic",
	})

	response := tester.HTTPServer(t).DoWithAuth(t, r)
	// response := srv.DoWithAuth(t, r)
	// response := tester.HTTPRequestWitAuth(t, r)
	require.Equal(t, http.StatusCreated, response.StatusCode)
}

// Verifies that http.BadRequest is returned when leaving out the required
// topic-name query parameter.
func TestAddRecordMissingTopic(t *testing.T) {
	r := httptest.NewRequest("POST", "/record", bytes.NewReader(nil))
	httphelpers.AddQueryParams(r, map[string]string{
		// NOTE: no topic-name set
	})

	response := tester.HTTPServer(t).DoWithAuth(t, r)
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
}
