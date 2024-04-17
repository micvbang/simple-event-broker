package tester

import (
	"context"
	"fmt"
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

func HTTPRequest(t *testing.T, r *http.Request) *httptest.ResponseRecorder {
	res, err := httpRequest(t, r)
	require.NoError(t, err)

	return res
}

func httpRequest(t *testing.T, r *http.Request) (_ *httptest.ResponseRecorder, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf(
				"http request \"%s %+v\" recovered from error: \"%s\"\nTIP: Check if you mocked authentication\nTIP: check if your dependencies are set correctly",
				r.Method, r.URL, v)
		}
	}()

	server := httpServer(t, DefaultAPIKey)

	w := httptest.NewRecorder()
	server.Mux.ServeHTTP(w, r)
	return w, err
}

type HTTPTestServer struct {
	Server *httptest.Server

	Mux       *http.ServeMux
	DiskCache *storage.DiskCache
	Storage   *storage.Storage
}

// HTTPServer calls HTTPServerWithAPIKey, using DefaultAPIKey.
func HTTPServer(t *testing.T) HTTPTestServer {
	return httpServer(t, DefaultAPIKey)
}

// HTTPServer initializes and returns an HTTPTestServer with all routes
// registered and HTTP endpoint dependencies created. The created dependencies
// can be useful during testing and are accessible on the HTTPTestServer struct.
func HTTPServerWithAPIKey(t *testing.T, apiKey string) HTTPTestServer {
	return httpServer(t, apiKey)
}

func httpServer(t *testing.T, apiKey string) HTTPTestServer {
	log := logger.NewDefault(context.Background())
	mux := http.NewServeMux()

	diskCache, err := storage.NewDiskCacheDefault(log, TempDir(t))
	require.NoError(t, err)

	topicStorage := func(log logger.Logger, topicName string) (*storage.TopicStorage, error) {
		memoryTopicStorage := storage.NewMemoryTopicStorage(log)
		return storage.NewTopicStorage(log, memoryTopicStorage, "", topicName, diskCache)
	}

	batcher := func(l logger.Logger, ts *storage.TopicStorage) storage.RecordBatcher {
		return recordbatch.NewNullBatcher(ts.AddRecordBatch)
	}

	storage := storage.New(log, topicStorage, batcher)
	sebhttp.RegisterRoutes(log, mux, storage, apiKey)

	return HTTPTestServer{
		Server:    httptest.NewServer(mux),
		Mux:       mux,
		DiskCache: diskCache,
		Storage:   storage,
	}
}
