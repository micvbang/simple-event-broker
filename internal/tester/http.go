package tester

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/sebhttp"
	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/stretchr/testify/require"
)

const DefaultAPIKey = "api-key"

type HTTPTestServer struct {
	t      *testing.T
	Server *httptest.Server

	Mux     *http.ServeMux
	Cache   *storage.Cache
	Storage *storage.Storage
}

func (s *HTTPTestServer) Do(t *testing.T, r *http.Request) *http.Response {
	return s.do(r, false)
}

func (s *HTTPTestServer) DoWithAuth(t *testing.T, r *http.Request) *http.Response {
	return s.do(r, true)
}

func (s *HTTPTestServer) do(r *http.Request, addDefaultAuth bool) *http.Response {
	if addDefaultAuth {
		r.Header.Add("Authorization", DefaultAPIKey)
	}

	w := httptest.NewRecorder()
	s.Mux.ServeHTTP(w, r)

	return w.Result()
}

// HTTPServer calls HTTPServerWithAPIKey, using DefaultAPIKey.
func HTTPServer(t *testing.T) *HTTPTestServer {
	return httpServer(t, DefaultAPIKey)
}

// HTTPServer initializes and returns an HTTPTestServer with all routes
// registered and HTTP endpoint dependencies created. The created dependencies
// can be useful during testing and are accessible on the HTTPTestServer struct.
func HTTPServerWithAPIKey(t *testing.T, apiKey string) *HTTPTestServer {
	return httpServer(t, apiKey)
}

func httpServer(t *testing.T, apiKey string) *HTTPTestServer {
	t.Helper()

	log := logger.NewDefault(context.Background())
	mux := http.NewServeMux()

	cache, err := storage.NewCache(log, storage.NewMemoryCache(log))
	require.NoError(t, err)

	topicStorage := func(log logger.Logger, topicName string) (*storage.TopicStorage, error) {
		memoryTopicStorage := storage.NewMemoryTopicStorage(log)
		return storage.NewTopicStorage(log, memoryTopicStorage, "", topicName, cache, nil)
	}

	batcher := func(l logger.Logger, ts *storage.TopicStorage) storage.RecordBatcher {
		return recordbatch.NewNullBatcher(ts.AddRecordBatch)
	}

	storage := storage.New(log, topicStorage, batcher)
	sebhttp.RegisterRoutes(log, mux, storage, apiKey)

	return &HTTPTestServer{
		t:       t,
		Server:  httptest.NewServer(mux),
		Mux:     mux,
		Cache:   cache,
		Storage: storage,
	}
}
