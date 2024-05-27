package httphandlers_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/micvbang/go-helpy/timey"
	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
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
	defer server.Close()

	for range topicRecords {
		_, err := server.Broker.AddRecord(topicName, tester.RandomBytes(t, 32))
		require.NoError(t, err)
	}
	expectedMetadata, err := server.Broker.Metadata(topicName)
	require.NoError(t, err)

	tests := map[string]struct {
		topicName string
		metadata  sebtopic.Metadata
	}{
		"empty":      {topicName: "has-no-records", metadata: sebtopic.Metadata{}},
		"one record": {topicName: topicName, metadata: expectedMetadata},
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
			require.Equal(t, test.metadata.NextOffset, output.NextOffset)
			require.True(t, timey.DiffEqual(10*time.Millisecond, test.metadata.LatestCommitAt, output.LatestCommitAt))
			require.Equal(t, "application/json", response.Header.Get("Content-Type"))
		})
	}
}

// TestGetTopicNotFound verifies that GET /topic returns the expected status
// code when fetching topic metadata, both with topic auto creation on and off.
func TestGetTopicNotFound(t *testing.T) {
	r := httptest.NewRequest("GET", "/topic", nil)
	httphelpers.AddQueryParams(r, map[string]string{
		"topic-name": "does-not-exist",
	})

	tests := map[string]struct {
		autoCreateTopic bool
		statusCode      int
	}{
		"auto create":    {autoCreateTopic: true, statusCode: http.StatusOK},
		"no auto create": {autoCreateTopic: false, statusCode: http.StatusNotFound},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			server := tester.HTTPServer(t, tester.HTTPStorageAutoCreateTopic(test.autoCreateTopic))
			defer server.Close()

			// Act
			response := server.DoWithAuth(r)

			// Assert
			require.Equal(t, test.statusCode, response.StatusCode)
		})
	}
}
