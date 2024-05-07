package sebhttp

import (
	"context"
	"crypto/subtle"
	"net/http"

	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/storage"
)

func RegisterRoutes(log logger.Logger, mux *http.ServeMux, storage *storage.Storage, apiKey string) {
	// TODO: we don't want something more secure and easier to manage than a
	// single, static API key.
	apiKeyBs := []byte(apiKey)

	requireAPIKey := NewAPIKeyHandler(log.Name("api key handler"), func(ctx context.Context, apiKey string) (bool, error) {
		apiKeyIsValid := subtle.ConstantTimeCompare(apiKeyBs, []byte(apiKey)) == 1
		return apiKeyIsValid, nil
	})

	mux.HandleFunc("POST /record", requireAPIKey(httphandlers.AddRecord(log, storage)))
	mux.HandleFunc("GET /record", requireAPIKey(httphandlers.GetRecord(log, storage)))
	mux.HandleFunc("GET /records", requireAPIKey(httphandlers.GetRecords(log, storage)))
	mux.HandleFunc("GET /topic", requireAPIKey(httphandlers.GetTopic(log, storage)))
}
