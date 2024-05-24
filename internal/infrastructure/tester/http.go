package tester

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/cache"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebhttp"
	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/micvbang/simple-event-broker/internal/topic"
	"github.com/stretchr/testify/require"
)

const DefaultAPIKey = "api-key"

type HTTPTestServer struct {
	t      *testing.T
	Server *httptest.Server

	Mux     *http.ServeMux
	Cache   *cache.Cache
	Storage *storage.Storage
}

// Close closes all of the underlying resources
func (s *HTTPTestServer) Close() {
	s.Server.Close()
}

func (s *HTTPTestServer) Do(r *http.Request) *http.Response {
	return s.do(r, false)
}

func (s *HTTPTestServer) DoWithAuth(r *http.Request) *http.Response {
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

// HTTPServer starts an HTTP test server using the given config.
func HTTPServer(t *testing.T, confs ...func(*httpServerConfig)) *HTTPTestServer {
	config := httpServerConfig{
		apiKey:                 DefaultAPIKey,
		storageTopicAutoCreate: true,
	}
	for _, configure := range confs {
		configure(&config)
	}

	return httpServer(t, config)
}

func httpServer(t *testing.T, config httpServerConfig) *HTTPTestServer {
	t.Helper()

	log := logger.NewDefault(context.Background())
	mux := http.NewServeMux()

	cache, err := cache.New(log, cache.NewMemoryStorage(log))
	require.NoError(t, err)

	topicFactory := func(log logger.Logger, topicName string) (*topic.Topic, error) {
		memoryTopicStorage := topic.NewMemoryStorage(log)
		return topic.New(log, memoryTopicStorage, topicName, cache, nil)
	}

	storage := storage.New(
		log,
		topicFactory,
		storage.WithNullBatcher(),
		storage.WithAutoCreateTopic(config.storageTopicAutoCreate),
	)
	sebhttp.RegisterRoutes(log, mux, storage, config.apiKey)

	return &HTTPTestServer{
		t:       t,
		Server:  httptest.NewServer(mux),
		Mux:     mux,
		Cache:   cache,
		Storage: storage,
	}
}

type httpServerConfig struct {
	apiKey                 string
	storageTopicAutoCreate bool
}

// HTTPAPIKey sets the apiKey for HTTPServer
func HTTPAPIKey(apiKey string) func(*httpServerConfig) {
	return func(c *httpServerConfig) {
		c.apiKey = apiKey
	}
}

// HTTPStorageAutoCreateTopic sets automatic topic creation for HTTPServer
func HTTPStorageAutoCreateTopic(autoCreate bool) func(*httpServerConfig) {
	return func(c *httpServerConfig) {
		c.storageTopicAutoCreate = autoCreate
	}
}
