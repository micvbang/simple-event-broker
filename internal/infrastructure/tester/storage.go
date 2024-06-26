package tester

import (
	"context"
	"fmt"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebbroker"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/stretchr/testify/require"
)

var (
	log logger.Logger = logger.NewDefault(context.Background())

	cacheStorageFactories = map[string]func(t *testing.T) (sebcache.Storage, error){
		"memory": func(t *testing.T) (sebcache.Storage, error) { return sebcache.NewMemoryStorage(log), nil },
		"disk":   func(t *testing.T) (sebcache.Storage, error) { return sebcache.NewDiskStorage(log, t.TempDir()) },
	}

	storageFactories = map[string]func(t *testing.T) sebtopic.Storage{
		"memory": func(t *testing.T) sebtopic.Storage { return sebtopic.NewMemoryStorage(log) },
		"disk":   func(t *testing.T) sebtopic.Storage { return sebtopic.NewDiskStorage(log, t.TempDir()) },
	}
)

// TestCacheStorage makes it easy to test all cache.CacheStorage
// implementations in the same test.
func TestCacheStorage(t *testing.T, f func(*testing.T, sebcache.Storage)) {
	t.Helper()

	for testName, cacheStorageFactory := range cacheStorageFactories {
		t.Run(testName, func(t *testing.T) {
			cacheStorage, err := cacheStorageFactory(t)
			require.NoError(t, err)

			f(t, cacheStorage)
		})
	}
}

// TestCacheStorage makes it easy to test all topic.BackingStorage
// implementations in the same test.
func TestBackingStorage(t *testing.T, f func(*testing.T, sebtopic.Storage)) {
	t.Helper()

	for storageName, backingStorageFactory := range storageFactories {
		t.Run(fmt.Sprintf("storage:%s", storageName), func(t *testing.T) {
			f(t, backingStorageFactory(t))
		})
	}
}

// TestTopicStorageAndCache makes it easy to test all topic.BackingStorage
// and cache.CacheStorage implementations in the same test.
func TestTopicStorageAndCache(t *testing.T, f func(*testing.T, sebtopic.Storage, *sebcache.Cache)) {
	t.Helper()

	for topicStorageName, topicStorageFactory := range storageFactories {
		for cacheName, cacheStorageFactory := range cacheStorageFactories {
			t.Run(fmt.Sprintf("storage:%s/cache:%s", topicStorageName, cacheName), func(t *testing.T) {
				cacheStorage, err := cacheStorageFactory(t)
				require.NoError(t, err)

				cache, err := sebcache.New(log, cacheStorage)
				require.NoError(t, err)

				f(t, topicStorageFactory(t), cache)
			})
		}
	}
}

// TestBroker makes it easy to test sebbroker.Broker with all configurations of
// sebtopic.Storage and sebcache.Storage implementations in the same test.
func TestBroker(t *testing.T, autoCreateTopic bool, f func(*testing.T, *sebbroker.Broker)) {
	t.Helper()

	for storageName, backingStorageFactory := range storageFactories {
		for cacheName, cacheStorageFactory := range cacheStorageFactories {
			t.Run(fmt.Sprintf("storage:%s/cache:%s", storageName, cacheName), func(t *testing.T) {
				cacheStorage, err := cacheStorageFactory(t)
				require.NoError(t, err)

				cache, err := sebcache.New(log, cacheStorage)
				require.NoError(t, err)

				broker := sebbroker.New(log,
					func(log logger.Logger, topicName string) (*sebtopic.Topic, error) {
						bs := backingStorageFactory(t)
						return sebtopic.New(log, bs, topicName, cache)
					},
					sebbroker.WithNullBatcher(),
					sebbroker.WithAutoCreateTopic(autoCreateTopic),
				)
				f(t, broker)
			})
		}
	}
}
