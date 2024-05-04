package tester

import (
	"context"
	"fmt"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/stretchr/testify/require"
)

var (
	log logger.Logger = logger.NewDefault(context.Background())

	cacheStorageFactories = map[string]func(t *testing.T) storage.CacheStorage{
		"memory": func(t *testing.T) storage.CacheStorage { return storage.NewMemoryCache(log) },
		"disk":   func(t *testing.T) storage.CacheStorage { return storage.NewDiskCache(log, t.TempDir()) },
	}

	storageFactories = map[string]func(t *testing.T) storage.BackingStorage{
		"memory": func(t *testing.T) storage.BackingStorage { return storage.NewMemoryTopicStorage(log) },
		"disk":   func(t *testing.T) storage.BackingStorage { return storage.NewDiskTopicStorage(log, t.TempDir()) },
	}
)

// TestCacheStorage makes it easy to test all storage.CacheStorage
// implementations in the same test.
func TestCacheStorage(t *testing.T, f func(*testing.T, storage.CacheStorage)) {
	t.Helper()

	for testName, cacheStorageFactory := range cacheStorageFactories {
		t.Run(testName, func(t *testing.T) {
			f(t, cacheStorageFactory(t))
		})
	}
}

// TestCacheStorage makes it easy to test all storage.BackingStorage
// implementations in the same test.
func TestBackingStorage(t *testing.T, f func(*testing.T, storage.BackingStorage)) {
	t.Helper()

	for storageName, backingStorageFactory := range storageFactories {
		t.Run(fmt.Sprintf("storage:%s", storageName), func(t *testing.T) {
			f(t, backingStorageFactory(t))
		})
	}
}

// TestBackingStorageAndCache makes it easy to test all storage.BackingStorage
// and storage.CacheStorage implementations in the same test.
func TestBackingStorageAndCache(t *testing.T, f func(*testing.T, storage.BackingStorage, *storage.Cache)) {
	t.Helper()

	for storageName, backingStorageFactory := range storageFactories {
		for cacheName, cacheStorageFactory := range cacheStorageFactories {
			t.Run(fmt.Sprintf("storage:%s/cache:%s", storageName, cacheName), func(t *testing.T) {
				cache, err := storage.NewCache(log, cacheStorageFactory(t))
				require.NoError(t, err)

				f(t, backingStorageFactory(t), cache)
			})
		}
	}
}
