package sebcache_test

import (
	"context"
	"io"
	"os"
	"path"
	"sort"
	"testing"

	"github.com/micvbang/go-helpy/mapy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/stretchr/testify/require"
)

var (
	log = logger.NewDefault(context.Background())
)

// TestCacheWriterWritesToDisk verifies that the io.WriteCloser returned by
// Writer() writes the expected bytes to the cache file, and does not write the
// file to the cache destination until Close() is called.
func TestCacheWriterWritesToDisk(t *testing.T) {
	tempDir := t.TempDir()

	const key = "some/topic/name/123"
	expectedCachedFile := path.Join(tempDir, key)

	expectedBytes := tester.RandomBytes(t, 4096)

	cache, err := sebcache.NewDiskStorage(log, tempDir)
	require.NoError(t, err)

	// Act
	f, err := cache.Writer(key)
	require.NoError(t, err)

	n, err := f.Write(expectedBytes)
	require.NoError(t, err)
	require.Equal(t, len(expectedBytes), n)

	// Assert
	// Write() must not write to cache dir until Close() has been called
	require.NoFileExists(t, expectedCachedFile)

	// must close file and move it to the actual cache dir.
	err = f.Close()
	require.NoError(t, err)
	require.FileExists(t, expectedCachedFile)

	// written bytes must be the same as the ones we wrote
	expectedFile, err := os.Open(expectedCachedFile)
	require.NoError(t, err)

	gotBytes, err := io.ReadAll(expectedFile)
	require.NoError(t, err)
	require.Equal(t, expectedBytes, gotBytes)
}

// TestCacheReaderReadsFromDisk verifies that the io.ReadSeekCloser returned by
// Reader() reads the correct file and returns the expected bytes.
func TestDiskCacheReaderReadsFromDisk(t *testing.T) {
	const key = "some/topic/name/123"

	expectedBytes := tester.RandomBytes(t, 4096)

	cache, err := sebcache.NewDiskStorage(log, t.TempDir())
	require.NoError(t, err)

	w, err := cache.Writer(key)
	require.NoError(t, err)

	tester.WriteAndClose(t, w, expectedBytes)

	// Act
	reader, err := cache.Reader(key)
	require.NoError(t, err)

	// Assert
	gotBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, expectedBytes, gotBytes)
}

// TestDiskCacheListFromDisk verifies that List() returns the expected keys,
// without any rootDir prefix.
func TestDiskCacheListFromDisk(t *testing.T) {
	cache, err := sebcache.NewDiskStorage(log, t.TempDir())
	require.NoError(t, err)

	expectedKeys := []string{
		"topic1/file1",
		"topic1/file2",
		"topic2/file1",
		"topic2/file2",
	}
	for _, key := range expectedKeys {
		w, err := cache.Writer(key)
		require.NoError(t, err)

		tester.WriteAndClose(t, w, tester.RandomBytes(t, 16))
	}

	// Act
	items, err := cache.List()
	require.NoError(t, err)

	gotKeys := mapy.Map(items, func(_ string, item sebcache.CacheItem) string {
		return item.Key
	})

	// Assert
	sort.Strings(expectedKeys)
	sort.Strings(gotKeys)
	require.Equal(t, expectedKeys, gotKeys)
}

// TestDiskCacheListFromDiskEmpty verifies that List() returns an empty list of
// keys when the cache is empty.
func TestDiskCacheListFromDiskEmpty(t *testing.T) {
	cache, err := sebcache.NewDiskStorage(log, t.TempDir())
	require.NoError(t, err)

	// Act
	items, err := cache.List()
	require.NoError(t, err)

	gotKeys := mapy.Map(items, func(_ string, item sebcache.CacheItem) string {
		return item.Key
	})

	// Assert
	require.Equal(t, []string{}, gotKeys)
}

// TestDiskCacheListExisting verifies that List returns the expected keys when
// listing an existing cache.
// NOTE: this is a regression test for a bug found on 2024-05-21, where the
// file's full path was used as the key, instead of just the key that was given
// by the caller.
func TestDiskCacheListExistingCache(t *testing.T) {
	rootDir := t.TempDir()
	cache, err := sebcache.NewDiskStorage(log, rootDir)
	require.NoError(t, err)

	const theKey = "Vrvirksomhed/000000000000.record_batch"
	w, err := cache.Writer(theKey)
	require.NoError(t, err)

	tester.WriteAndClose(t, w, tester.RandomBytes(t, 16))

	c2, err := sebcache.NewDiskStorage(log, rootDir)
	require.NoError(t, err)

	// Act
	items, err := c2.List()
	require.NoError(t, err)

	// Assert
	require.Equal(t, 1, len(items))

	item := items[theKey]
	require.Equal(t, theKey, item.Key)
}
