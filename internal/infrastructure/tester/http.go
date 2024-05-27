package tester

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/cache"
	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebbroker"
	"github.com/micvbang/simple-event-broker/internal/topic"
	"github.com/stretchr/testify/require"
)

const DefaultAPIKey = "api-key"

type HTTPTestServer struct {
	t      *testing.T
	Server *httptest.Server

	Mux    *http.ServeMux
	Cache  *cache.Cache
	Broker *sebbroker.Broker
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
func HTTPServer(t *testing.T, OptFns ...func(*Opts)) *HTTPTestServer {
	t.Helper()
	opts := Opts{
		APIKey:                 DefaultAPIKey,
		StorageTopicAutoCreate: true,
	}
	for _, optFn := range OptFns {
		optFn(&opts)
	}

	log := logger.NewDefault(context.Background())

	var c *cache.Cache
	var s *sebbroker.Broker
	var err error

	if opts.Dependencies == nil {
		c, err = cache.New(log, cache.NewMemoryStorage(log))
		require.NoError(t, err)

		topicFactory := func(log logger.Logger, topicName string) (*topic.Topic, error) {
			memoryTopicStorage := topic.NewMemoryStorage(log)
			return topic.New(log, memoryTopicStorage, topicName, c, topic.WithCompress(nil))
		}

		s = sebbroker.New(
			log,
			topicFactory,
			sebbroker.WithNullBatcher(),
			sebbroker.WithAutoCreateTopic(opts.StorageTopicAutoCreate),
		)
		opts.Dependencies = s
	}

	mux := http.NewServeMux()

	httphandlers.RegisterRoutes(log, mux, opts.Dependencies, opts.APIKey)

	return &HTTPTestServer{
		t:      t,
		Server: httptest.NewServer(mux),
		Mux:    mux,
		Cache:  c,
		Broker: s,
	}
}

type Opts struct {
	APIKey                 string
	StorageTopicAutoCreate bool
	Dependencies           httphandlers.Dependencies
}

// HTTPAPIKey sets the apiKey for HTTPServer
func HTTPAPIKey(apiKey string) func(*Opts) {
	return func(c *Opts) {
		c.APIKey = apiKey
	}
}

// HTTPStorageAutoCreateTopic sets automatic topic creation for HTTPServer
func HTTPStorageAutoCreateTopic(autoCreate bool) func(*Opts) {
	return func(c *Opts) {
		c.StorageTopicAutoCreate = autoCreate
	}
}

// HTTPDependencies sets the http dependencies, avoiding creation of the
// defaults.
//
// This is mostly useful when mocking is required to make a test possible to
// test. Otherwise it's generally preferred to just set up the required state
// using the default dependencies.
func HTTPDependencies(deps httphandlers.Dependencies) func(*Opts) {
	return func(c *Opts) {
		c.Dependencies = deps
	}
}
