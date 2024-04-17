package storage_test

import (
	"io"
	"os"
	"path"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/storage"
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

	c := storage.NewDiskCache(log, tempDir)

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
func TestDiskCacheReaderReadsFromDisk(t *testing.T) {
	const cachePath = "/some/topic/name/123"

	expectedBytes := tester.RandomBytes(t, 4096)

	c := storage.NewDiskCache(log, tester.TempDir(t))

	w, err := c.Writer(cachePath)
	require.NoError(t, err)

	tester.WriteAndClose(t, w, expectedBytes)

	// Act
	reader, err := c.Reader(cachePath)
	require.NoError(t, err)

	// Assert
	gotBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, expectedBytes, gotBytes)
}
