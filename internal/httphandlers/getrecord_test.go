package httphandlers_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

// TestGetRecordHappyPath verifies that http.StatusNotFound is returned when
// either the topic name or record id does not exist.
func TestGetRecordExistence(t *testing.T) {
	server := tester.HTTPServer(t)

	expectedPayload := []byte("haps")
	const topicName = "topicName"

	recordID, err := server.Storage.AddRecord(topicName, expectedPayload)
	require.NoError(t, err)

	tests := map[string]struct {
		recordID   uint64
		topicName  string
		statusCode int
	}{
		"record not found": {
			recordID:   42,
			topicName:  topicName,
			statusCode: http.StatusNotFound,
		},
		"topic not found": {
			recordID:   recordID,
			topicName:  "does-not-exist",
			statusCode: http.StatusNotFound,
		},
		"found": {
			recordID:   recordID,
			topicName:  topicName,
			statusCode: http.StatusOK,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/record", nil)
			httphelpers.AddQueryParams(r, map[string]string{
				"topic-name": test.topicName,
				"record-id":  fmt.Sprintf("%d", test.recordID),
			})

			// Act
			response := server.DoWithAuth(r)

			// Assert
			require.Equal(t, test.statusCode, response.StatusCode)
		})
	}
}
