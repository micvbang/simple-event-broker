package cache_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/timey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/cache"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/stretchr/testify/require"
)

// TestCacheEvictLeastRecentlyUsed verifies that calls to EvictLeastRecentlyUsed
// removes items from the cache until there's at most the given number of bytes
// in the cache.
func TestCacheEvictLeastRecentlyUsed(t *testing.T) {
	tester.TestCacheStorage(t, func(t *testing.T, cacheStorage cache.Storage) {
		// mockTime is used to advance time to avoid clashes in cache item's
		// "accessedAt" value, which would effectively randomize the order in which
		// items should be evicted
		mockTime := timey.NewMockTime(nil)

		bs := tester.RandomBytes(t, 64)

		cacheItems := []struct {
			key string
			bs  []byte
		}{
			{"0", bs},
			{"1", bs},
			{"2", bs},
			{"3", bs},
			{"4", bs},
		}

		c, err := cache.NewCacheWithNow(log, cacheStorage, mockTime.Now)
		require.NoError(t, err)

		bytesCached := 0
		for _, item := range cacheItems {
			mockTime.Add(time.Second)

			_, err := c.Write(item.key, item.bs)
			require.NoError(t, err)

			bytesCached += len(item.bs)
		}
		require.Equal(t, int64(bytesCached), c.Size())

		// evict items one by one
		for i := 0; i < len(cacheItems); i++ {
			expectedSize := int64(len(bs) * (len(cacheItems) - i))

			// Act
			err := c.EvictLeastRecentlyUsed(expectedSize)
			require.NoError(t, err)

			gotSize := c.Size()

			// Assert
			require.Equal(t, expectedSize, gotSize)

			// most recently accessed items must be evicted
			for _, item := range cacheItems[:i] {
				_, err := c.Reader(item.key)
				require.ErrorIs(t, err, seb.ErrNotInCache)
			}

			// least recently accessed items must stay
			for _, item := range cacheItems[i:] {
				mockTime.Add(time.Second)

				// Act
				rdr, err := c.Reader(item.key)
				require.NoError(t, err)
				bs := tester.ReadAndClose(t, rdr)

				// Assert
				require.Equal(t, item.bs, bs)
			}
		}
	})
}

// TestCacheEvictLeastRecentlyUsedMaxBytes verifies that calls to
// EvictLeastRecentlyUsed ensures that the cache contains at most the given
// number of bytes, potentially evicting more than one item per call.
func TestCacheEvictLeastRecentlyUsedMaxBytes(t *testing.T) {
	tester.TestCacheStorage(t, func(t *testing.T, cacheStorage cache.Storage) {
		c, err := cache.New(log, cacheStorage)
		require.NoError(t, err)

		bs := tester.RandomBytes(t, 10)

		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("%d", i)

			_, err = c.Write(key, bs)
			require.NoError(t, err)
		}

		// remove no items
		err = c.EvictLeastRecentlyUsed(50)
		require.NoError(t, err)
		require.Equal(t, int64(50), c.Size())

		// remove one item
		err = c.EvictLeastRecentlyUsed(49)
		require.NoError(t, err)
		require.Equal(t, int64(40), c.Size())

		// remove two items
		err = c.EvictLeastRecentlyUsed(21)
		require.NoError(t, err)
		require.Equal(t, int64(20), c.Size())

		// remove all items
		err = c.EvictLeastRecentlyUsed(0)
		require.NoError(t, err)
		require.Equal(t, int64(0), c.Size())
	})
}

// TestCacheEvictLeastRecentlyEmptyCache verifies that calls to EvictLeastRecentlyUsed
// does not fail if the cache directory does not exist.
func TestCacheEvictLeastRecentlyEmptyCache(t *testing.T) {
	tester.TestCacheStorage(t, func(t *testing.T, cacheStorage cache.Storage) {
		mockTime := timey.NewMockTime(nil)

		c, err := cache.NewCacheWithNow(log, cacheStorage, mockTime.Now)
		require.NoError(t, err)

		err = c.EvictLeastRecentlyUsed(100)
		require.NoError(t, err)
	})
}

// TestCacheReaderFileNotCached verifies that Reader() returns ErrNotInCache
// when attempting to read a file from cache that does not exist.
func TestCacheReaderFileNotCached(t *testing.T) {
	tester.TestCacheStorage(t, func(t *testing.T, cacheStorage cache.Storage) {
		c, err := cache.New(log, cacheStorage)
		require.NoError(t, err)

		// Act, assert
		_, err = c.Reader("non/existing/path")
		require.ErrorIs(t, err, seb.ErrNotInCache)
	})
}

// TestCacheSize verifies that Size() returns the expected number of bytes.
func TestCacheSize(t *testing.T) {
	tester.TestCacheStorage(t, func(t *testing.T, cacheStorage cache.Storage) {
		c, err := cache.New(log, cacheStorage)
		require.NoError(t, err)

		expectedSize := 0
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("/some/name/%d", i)
			bs := tester.RandomBytes(t, 128+inty.RandomN(256))

			_, err := c.Write(key, bs)
			require.NoError(t, err)

			expectedSize += len(bs)
		}

		require.Equal(t, int64(expectedSize), c.Size())
	})
}

// TestCacheSizeWithExistingFiles verifies that Cache initializes the cache with
// data already on disk, such that calls to Size() returns the correct number of
// bytes currently on disk for the given root dir. Additionally, it's also
// verified that Size() still reports the correct number of bytes after adding
// _more_ items to a pre-initialized cache.
func TestCacheSizeWithExistingFiles(t *testing.T) {
	tester.TestCacheStorage(t, func(t *testing.T, cacheStorage cache.Storage) {
		expectedSize := 0
		{
			c1, err := cache.New(log, cacheStorage)
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				f, err := c1.Writer(fmt.Sprintf("/some/name/%d", i))
				require.NoError(t, err)

				bs := tester.RandomBytes(t, 128+inty.RandomN(256))
				tester.WriteAndClose(t, f, bs)

				expectedSize += len(bs)
			}

			require.Equal(t, int64(expectedSize), c1.Size())
		}

		// Act
		c2, err := cache.New(log, cacheStorage)
		require.NoError(t, err)

		// Assert
		require.Equal(t, int64(expectedSize), c2.Size())

		// Act
		// add more items to the cache
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("/some/other/name/%d", i)
			bs := tester.RandomBytes(t, 128+inty.RandomN(256))

			_, err := c2.Write(key, bs)
			require.NoError(t, err)

			expectedSize += len(bs)
		}

		// Assert
		require.Equal(t, int64(expectedSize), c2.Size())
	})
}

// TestCacheSizeOverwriteItem verifies that Size() returns the correct number of
// bytes when items in the cache are overwritten.
func TestCacheSizeOverwriteItem(t *testing.T) {
	tester.TestCacheStorage(t, func(t *testing.T, cacheStorage cache.Storage) {
		c, err := cache.New(log, cacheStorage)
		require.NoError(t, err)

		itemsToCache := [][]byte{
			tester.RandomBytes(t, 256), // put item in cache
			tester.RandomBytes(t, 128), // smaller item
			tester.RandomBytes(t, 512), // larger item
		}

		for _, item := range itemsToCache {
			// Act
			n, err := c.Write("overwritten-item", item)
			require.NoError(t, err)
			require.Equal(t, len(item), n)

			// Assert
			require.Equal(t, int64(len(item)), c.Size())
		}
	})
}

// TestCacheEvictAfterFailure verifies that EvictLeastRecentlyUsed releases
// its lock after an error has occurred in a previous call to it.
// This is a regression test for a bug that was spotted on 2024-05-08.
func TestCacheEvictAfterFailure(t *testing.T) {
	cacheStorage := &cacheStorageMock{}
	cacheStorage.MockRemove = func(key string) error {
		return fmt.Errorf("everything is on fire!")
	}
	cacheStorage.MockList = func() (map[string]cache.CacheItem, error) {
		cacheItems := map[string]cache.CacheItem{
			"key1": {
				Size: 42,
				Key:  "key1",
			},
		}

		return cacheItems, nil
	}

	c, err := cache.New(log, cacheStorage)
	require.NoError(t, err)

	// Act
	err = c.EvictLeastRecentlyUsed(1)
	require.Error(t, err)

	hasReturned := false

	// Assert
	go func() {
		c.EvictLeastRecentlyUsed(1)
		hasReturned = true
	}()

	require.Eventually(t, func() bool { return hasReturned }, 1*time.Second, 10*time.Millisecond)
}

type cacheStorageMock struct {
	cache.Storage

	MockRemove   func(key string) error
	RemoveCalled bool

	MockList   func() (map[string]cache.CacheItem, error)
	ListCalled bool
}

func (c *cacheStorageMock) Remove(key string) error {
	c.RemoveCalled = true
	return c.MockRemove(key)
}

func (c *cacheStorageMock) List() (map[string]cache.CacheItem, error) {
	c.ListCalled = true
	return c.MockList()
}