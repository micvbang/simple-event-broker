package httphelpers

import (
	"context"
	"math"
	"net/http"
	"strings"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

const (
	APIKeyHeader = "Authorization"
	bearerPrefix = "Bearer "
)

// NewAPIKeyHandler returns an http.HandlerFunc that can be used to wrap other
// http.HandlerFuncs. The returned http.HandlerFunc uses allowed to determine
// whether an incoming http.Request should have access to the wrapped endpoint.
func NewAPIKeyHandler(log logger.Logger, allowed func(ctx context.Context, apiKey string) (bool, error)) func(http.HandlerFunc) http.HandlerFunc {
	return func(hf http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			requestAPIKey := r.Header.Get(APIKeyHeader)
			if len(requestAPIKey) == 0 {
				invalidAuth(w, r)
				return
			}

			requestAPIKey = strings.TrimPrefix(requestAPIKey, bearerPrefix)

			apiKeyLogLength := inty.Min(int(math.Floor(float64(len(requestAPIKey))*0.5)), 10)
			log.Debugf("checking api key '%s' (len %d)", requestAPIKey[:apiKeyLogLength], len(requestAPIKey))
			ok, err := allowed(r.Context(), requestAPIKey)
			if err != nil {
				internalServerError(w, r)
				return
			}

			if !ok {
				log.Infof("invalid api key")
				invalidAuth(w, r)
				return
			}

			hf.ServeHTTP(w, r)
		}
	}
}

func invalidAuth(w http.ResponseWriter, r *http.Request) {
	r.Body.Close()
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte("invalid auth"))
}

func internalServerError(w http.ResponseWriter, r *http.Request) {
	r.Body.Close()
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte("something went wrong"))
}
