package httphandlers_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/stretchr/testify/require"
)

// TestGetTopicHappyPath verifies that GetTopic() returns the id of the next
// record added to the topic.
func TestGetTopicHappyPath(t *testing.T) {
	const (
		topicName    = "topicName"
		topicRecords = 10
	)

	server := tester.HTTPServer(t)
	for range topicRecords {
		_, err := server.Storage.AddRecord(topicName, tester.RandomBytes(t, 32))
		require.NoError(t, err)
	}

	tests := map[string]struct {
		topicName string
		offset    uint64
	}{
		"empty":      {topicName: "has-no-records", offset: 0},
		"one record": {topicName: topicName, offset: topicRecords},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/topic", nil)
			httphelpers.AddQueryParams(r, map[string]string{
				"topic-name": test.topicName,
			})

			// Act
			response := server.DoWithAuth(r)

			// Assert
			require.Equal(t, http.StatusOK, response.StatusCode)

			output := httphandlers.GetTopicOutput{}
			err := httphelpers.ParseJSONAndClose(response.Body, &output)
			require.NoError(t, err)
			require.Equal(t, test.offset, output.Offset)
			require.Equal(t, "application/json", response.Header.Get("Content-Type"))
		})
	}
}
