package storage

import (
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

// TestCacheWriterWritesToDisk verifies that the io.WriteCloser returned by
// Writer() writes the expected bytes to the cache file, and does not write the
// file to the cache destination until Close() is called.
func TestCacheWriterWritesToDisk(t *testing.T) {
	tempDir := tester.TempDir(t)

	const cachePath = "/some/topic/name/123"
	expectedCachedFile := path.Join(tempDir, cachePath)

	expectedBytes := tester.RandomBytes(t, 4096)

	c, err := NewDiskCache(log, tempDir)
	require.NoError(t, err)

	// Act
	f, err := c.Writer(cachePath)
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
func TestCacheReaderReadsFromDisk(t *testing.T) {
	tempDir := tester.TempDir(t)
	const cachePath = "/some/topic/name/123"

	expectedBytes := tester.RandomBytes(t, 4096)

	c, err := NewDiskCache(log, tempDir)
	require.NoError(t, err)

	f, err := c.Writer(cachePath)
	require.NoError(t, err)

	tester.WriteAndClose(t, f, expectedBytes)

	// Act
	reader, err := c.Reader(cachePath)
	require.NoError(t, err)

	// Assert
	gotBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, expectedBytes, gotBytes)
}

// TestCacheReaderFileNotCached verifies that Reader() returns ErrNotInCache
// when attempting to read a file from cache that does not exist.
func TestCacheReaderFileNotCached(t *testing.T) {
	tempDir := tester.TempDir(t)

	c, err := NewDiskCache(log, tempDir)
	require.NoError(t, err)

	// Act, assert
	_, err = c.Reader("non/existing/path")
	require.ErrorIs(t, err, ErrNotInCache)
}

// TestCacheSize verifies that Size() returns the expected number of bytes.
func TestCacheSize(t *testing.T) {
	tempDir := tester.TempDir(t)
	c, err := NewDiskCache(log, tempDir)
	require.NoError(t, err)

	expectedSize := 0
	for i := 0; i < 10; i++ {
		f, err := c.Writer(fmt.Sprintf("/some/name/%d", i))
		require.NoError(t, err)

		bs := tester.RandomBytes(t, 128+inty.RandomN(256))
		tester.WriteAndClose(t, f, bs)

		expectedSize += len(bs)
	}

	require.Equal(t, int64(expectedSize), c.Size())
}

// TestCacheSizeWithExistingFiles verifies that NewDiskCache initializes the
// cache with data already on disk, such that calls to Size() returns the
// correct number of bytes currently on disk for the given root dir.
// Additionally, it's also verified that Size() still reports the correct number
// of bytes after adding _more_ items to a pre-initialized cache.
func TestCacheSizeWithExistingFiles(t *testing.T) {
	tempDir := tester.TempDir(t)

	expectedSize := 0
	{
		c1, err := NewDiskCache(log, tempDir)
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
	c2, err := NewDiskCache(log, tempDir)
	require.NoError(t, err)

	// Assert
	require.Equal(t, int64(expectedSize), c2.Size())

	// Act
	// add more items to the cache
	for i := 0; i < 10; i++ {
		f, err := c2.Writer(fmt.Sprintf("/some/other/name/%d", i))
		require.NoError(t, err)

		bs := tester.RandomBytes(t, 128+inty.RandomN(256))
		tester.WriteAndClose(t, f, bs)

		expectedSize += len(bs)
	}

	// Assert
	require.Equal(t, int64(expectedSize), c2.Size())
}

// TestCacheSizeOverwriteItem verifies that Size() returns the correct number of
// bytes when items in the cache are overwritten.
func TestCacheSizeOverwriteItem(t *testing.T) {
	tempDir := tester.TempDir(t)
	c, err := NewDiskCache(log, tempDir)
	require.NoError(t, err)

	itemsToCache := [][]byte{
		tester.RandomBytes(t, 256), // put item in cache
		tester.RandomBytes(t, 128), // smaller item
		tester.RandomBytes(t, 512), // larger item
	}

	for _, item := range itemsToCache {
		// Act
		f, err := c.Writer("overwritten-item")
		require.NoError(t, err)
		tester.WriteAndClose(t, f, item)

		// Assert
		require.Equal(t, int64(len(item)), c.Size())
	}
}
