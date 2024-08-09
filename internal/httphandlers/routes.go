package httphandlers

import (
	"context"
	"crypto/subtle"
	"net/http"

	"github.com/micvbang/go-helpy/syncy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
)

//go:generate mocky -i Dependencies
type Dependencies interface {
	RecordsAdder
	RecordGetter
	RecordsGetter
	TopicGetter
}

func RegisterRoutes(log logger.Logger, mux *http.ServeMux, batchPool *syncy.Pool[*sebrecords.Batch], deps Dependencies, apiKey string) {
	// TODO: we want something more secure and easier to manage than a
	// single, static API key.
	apiKeyBs := []byte(apiKey)

	requireAPIKey := httphelpers.NewAPIKeyHandler(log.Name("api key handler"), func(ctx context.Context, apiKey string) (bool, error) {
		apiKeyIsValid := subtle.ConstantTimeCompare(apiKeyBs, []byte(apiKey)) == 1
		return apiKeyIsValid, nil
	})

	mux.HandleFunc("POST /records", requireAPIKey(AddRecords(log, batchPool, deps)))
	mux.HandleFunc("GET /record", requireAPIKey(GetRecord(log, deps)))
	mux.HandleFunc("GET /records", requireAPIKey(GetRecords(log, batchPool, deps)))
	mux.HandleFunc("GET /topic", requireAPIKey(GetTopic(log, deps)))
}
