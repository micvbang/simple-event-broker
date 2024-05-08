package tester

import (
	"context"
	"fmt"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/cache"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/micvbang/simple-event-broker/internal/topic"
	"github.com/stretchr/testify/require"
)

var (
	log logger.Logger = logger.NewDefault(context.Background())

	cacheStorageFactories = map[string]func(t *testing.T) cache.Storage{
		"memory": func(t *testing.T) cache.Storage { return cache.NewMemoryStorage(log) },
		"disk":   func(t *testing.T) cache.Storage { return cache.NewDiskStorage(log, t.TempDir()) },
	}

	storageFactories = map[string]func(t *testing.T) topic.Storage{
		"memory": func(t *testing.T) topic.Storage { return topic.NewMemoryStorage(log) },
		"disk":   func(t *testing.T) topic.Storage { return topic.NewDiskStorage(log, t.TempDir()) },
	}
)

// TestCacheStorage makes it easy to test all cache.CacheStorage
// implementations in the same test.
func TestCacheStorage(t *testing.T, f func(*testing.T, cache.Storage)) {
	t.Helper()

	for testName, cacheStorageFactory := range cacheStorageFactories {
		t.Run(testName, func(t *testing.T) {
			f(t, cacheStorageFactory(t))
		})
	}
}

// TestCacheStorage makes it easy to test all topic.BackingStorage
// implementations in the same test.
func TestBackingStorage(t *testing.T, f func(*testing.T, topic.Storage)) {
	t.Helper()

	for storageName, backingStorageFactory := range storageFactories {
		t.Run(fmt.Sprintf("storage:%s", storageName), func(t *testing.T) {
			f(t, backingStorageFactory(t))
		})
	}
}

// TestBackingStorageAndCache makes it easy to test all topic.BackingStorage
// and cache.CacheStorage implementations in the same test.
func TestBackingStorageAndCache(t *testing.T, f func(*testing.T, topic.Storage, *cache.Cache)) {
	t.Helper()

	for topicStorageName, topicStorageFactory := range storageFactories {
		for cacheName, cacheStorageFactory := range cacheStorageFactories {
			t.Run(fmt.Sprintf("storage:%s/cache:%s", topicStorageName, cacheName), func(t *testing.T) {
				cache, err := cache.New(log, cacheStorageFactory(t))
				require.NoError(t, err)

				f(t, topicStorageFactory(t), cache)
			})
		}
	}
}

// TestBackingStorageAndCache makes it easy to storage.Storage with all
// configurations of topic.BackingStorage and cache.CacheStorage
// implementations in the same test.
func TestStorage(t *testing.T, autoCreateTopic bool, f func(*testing.T, *storage.Storage)) {
	t.Helper()

	for storageName, backingStorageFactory := range storageFactories {
		for cacheName, cacheStorageFactory := range cacheStorageFactories {
			t.Run(fmt.Sprintf("storage:%s/cache:%s", storageName, cacheName), func(t *testing.T) {
				cache, err := cache.New(log, cacheStorageFactory(t))
				require.NoError(t, err)

				s := storage.NewWithAutoCreate(log,
					func(log logger.Logger, topicName string) (*topic.Topic, error) {
						bs := backingStorageFactory(t)
						return topic.New(log, bs, topicName, cache, &topic.Gzip{})
					},
					func(l logger.Logger, t *topic.Topic) storage.RecordBatcher {
						return storage.NewNullBatcher(t.AddRecordBatch)
					},
					autoCreateTopic,
				)
				f(t, s)
			})
		}
	}
}
