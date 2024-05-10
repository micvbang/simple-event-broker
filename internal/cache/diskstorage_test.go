package cache_test

import (
	"context"
	"io"
	"os"
	"path"
	"sort"
	"testing"

	"github.com/micvbang/go-helpy/mapy"
	"github.com/micvbang/simple-event-broker/internal/cache"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
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

	c, err := cache.NewDiskStorage(log, tempDir)
	require.NoError(t, err)

	// Act
	f, err := c.Writer(key)
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

	c, err := cache.NewDiskStorage(log, t.TempDir())
	require.NoError(t, err)

	w, err := c.Writer(key)
	require.NoError(t, err)

	tester.WriteAndClose(t, w, expectedBytes)

	// Act
	reader, err := c.Reader(key)
	require.NoError(t, err)

	// Assert
	gotBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, expectedBytes, gotBytes)
}

// TestDiskCacheListFromDisk verifies that List() returns the expected keys,
// without any rootDir prefix.
func TestDiskCacheListFromDisk(t *testing.T) {
	c, err := cache.NewDiskStorage(log, t.TempDir())
	require.NoError(t, err)

	expectedKeys := []string{
		"topic1/file1",
		"topic1/file2",
		"topic2/file1",
		"topic2/file2",
	}
	for _, key := range expectedKeys {
		w, err := c.Writer(key)
		require.NoError(t, err)

		tester.WriteAndClose(t, w, tester.RandomBytes(t, 16))
	}

	// Act
	items, err := c.List()
	require.NoError(t, err)

	gotKeys := mapy.Map(items, func(_ string, item cache.CacheItem) string {
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
	c, err := cache.NewDiskStorage(log, t.TempDir())
	require.NoError(t, err)

	// Act
	items, err := c.List()
	require.NoError(t, err)

	gotKeys := mapy.Map(items, func(_ string, item cache.CacheItem) string {
		return item.Key
	})

	// Assert
	require.Equal(t, []string{}, gotKeys)
}
