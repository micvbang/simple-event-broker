package httphandlers_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/stretchr/testify/require"
)

// TestGetRecordExistence verifies that http.StatusNotFound is returned when
// either the topic name or offset does not exist.
func TestGetRecordExistence(t *testing.T) {
	server := tester.HTTPServer(t)
	defer server.Close()

	const topicName = "topicName"

	offsets, err := server.Broker.AddRecords(topicName, tester.MakeRandomRecordBatch(1))
	require.NoError(t, err)

	tests := map[string]struct {
		offset     uint64
		topicName  string
		statusCode int
	}{
		"record not found": {
			offset:     42,
			topicName:  topicName,
			statusCode: http.StatusNotFound,
		},
		"topic not found": {
			offset:     offsets[0],
			topicName:  "does-not-exist",
			statusCode: http.StatusNotFound,
		},
		"found": {
			offset:     offsets[0],
			topicName:  topicName,
			statusCode: http.StatusOK,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/record", nil)
			httphelpers.AddQueryParams(r, map[string]string{
				"topic-name": test.topicName,
				"offset":     fmt.Sprintf("%d", test.offset),
			})

			// Act
			response := server.DoWithAuth(r)

			// Assert
			require.Equal(t, test.statusCode, response.StatusCode)
		})
	}
}
