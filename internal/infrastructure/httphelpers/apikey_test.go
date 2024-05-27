package httphelpers_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/stretchr/testify/require"
)

var log = logger.NewDefault(context.Background())

// TestAPIKeyAPIKeyValidity verifies that APIKeyHandler blocks requests that
// do not provide a valid API key with StatusUnauthorized.
func TestAPIKeyAPIKeyValidity(t *testing.T) {
	const workingAPIKey = "working API key"

	requireAPIKey := httphelpers.NewAPIKeyHandler(log, func(ctx context.Context, apiKey string) (bool, error) {
		return apiKey == workingAPIKey, nil
	})

	httpHandler := requireAPIKey(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	tests := map[string]struct {
		apiKeyHeader       string
		apiKey             string
		expectedStatusCode int
	}{
		"happy path with bearer": {
			apiKeyHeader:       httphelpers.APIKeyHeader,
			apiKey:             "Bearer working API key",
			expectedStatusCode: http.StatusOK,
		},
		"happy path without bearer": {
			apiKeyHeader:       httphelpers.APIKeyHeader,
			apiKey:             workingAPIKey,
			expectedStatusCode: http.StatusOK,
		},
		"incorrect header": {
			apiKeyHeader:       "not-the-api-key-header",
			apiKey:             workingAPIKey,
			expectedStatusCode: http.StatusUnauthorized,
		},
		"invalid api key message": {
			apiKeyHeader:       "not-the-api-key-header",
			apiKey:             workingAPIKey,
			expectedStatusCode: http.StatusUnauthorized,
		},
		"incorrect api key": {
			apiKeyHeader:       httphelpers.APIKeyHeader,
			apiKey:             "not an api key",
			expectedStatusCode: http.StatusUnauthorized,
		},
		"invalid bearer, missing space": {
			apiKeyHeader:       httphelpers.APIKeyHeader,
			apiKey:             "Bearerworking API key",
			expectedStatusCode: http.StatusUnauthorized,
		},
		"invalid bearer, using colon": {
			apiKeyHeader:       httphelpers.APIKeyHeader,
			apiKey:             "Bearer: working API key",
			expectedStatusCode: http.StatusUnauthorized,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://test/", nil)
			r.Header.Set(test.apiKeyHeader, test.apiKey)

			w := httptest.NewRecorder()

			httpHandler(w, r)
			require.Equal(t, test.expectedStatusCode, w.Code)
		})
	}
}
