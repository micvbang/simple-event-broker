package httphandlers

import (
	"context"
	"crypto/subtle"
	"net/http"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

//go:generate mocky -i Dependencies
type Dependencies interface {
	RecordAdder
	RecordsAdder
	RecordGetter
	RecordsGetter
	TopicGetter
}

func RegisterRoutes(log logger.Logger, mux *http.ServeMux, deps Dependencies, apiKey string) {
	// TODO: we want something more secure and easier to manage than a
	// single, static API key.
	apiKeyBs := []byte(apiKey)

	requireAPIKey := httphelpers.NewAPIKeyHandler(log.Name("api key handler"), func(ctx context.Context, apiKey string) (bool, error) {
		apiKeyIsValid := subtle.ConstantTimeCompare(apiKeyBs, []byte(apiKey)) == 1
		return apiKeyIsValid, nil
	})

	mux.HandleFunc("POST /record", requireAPIKey(AddRecord(log, deps)))
	mux.HandleFunc("POST /records", requireAPIKey(AddRecords(log, deps)))
	mux.HandleFunc("GET /record", requireAPIKey(GetRecord(log, deps)))
	mux.HandleFunc("GET /records", requireAPIKey(GetRecords(log, deps)))
	mux.HandleFunc("GET /topic", requireAPIKey(GetTopic(log, deps)))
}
