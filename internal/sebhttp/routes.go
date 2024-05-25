package sebhttp

import (
	"context"
	"crypto/subtle"
	"net/http"

	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

//go:generate mocky -i Dependencies
type Dependencies interface {
	httphandlers.RecordAdder
	httphandlers.RecordsAdder
	httphandlers.RecordGetter
	httphandlers.RecordsGetter
	httphandlers.TopicGetter
}

func RegisterRoutes(log logger.Logger, mux *http.ServeMux, deps Dependencies, apiKey string) {
	// TODO: we want something more secure and easier to manage than a
	// single, static API key.
	apiKeyBs := []byte(apiKey)

	requireAPIKey := NewAPIKeyHandler(log.Name("api key handler"), func(ctx context.Context, apiKey string) (bool, error) {
		apiKeyIsValid := subtle.ConstantTimeCompare(apiKeyBs, []byte(apiKey)) == 1
		return apiKeyIsValid, nil
	})

	mux.HandleFunc("POST /record", requireAPIKey(httphandlers.AddRecord(log, deps)))
	mux.HandleFunc("POST /records", requireAPIKey(httphandlers.AddRecords(log, deps)))
	mux.HandleFunc("GET /record", requireAPIKey(httphandlers.GetRecord(log, deps)))
	mux.HandleFunc("GET /records", requireAPIKey(httphandlers.GetRecords(log, deps)))
	mux.HandleFunc("GET /topic", requireAPIKey(httphandlers.GetTopic(log, deps)))
}
