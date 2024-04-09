package storage

import (
	"crypto/rand"
	"io"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCacheWriterWritesToDisk verifies that the io.WriteCloser returned by
// Writer() writes the expected bytes to the cache file, and does not write the
// file to the cache destination until Close() is called.
func TestCacheWriterWritesToDisk(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	const cachePath = "/some/topic/name/123"
	expectedCachedFile := path.Join(tempDir, cachePath)

	expectedBytes := make([]byte, 4096)
	_, err = rand.Read(expectedBytes)
	require.NoError(t, err)

	c := NewDiskCache(log, tempDir)

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
	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	const cachePath = "/some/topic/name/123"

	expectedBytes := make([]byte, 4096)
	_, err = rand.Read(expectedBytes)
	require.NoError(t, err)

	c := NewDiskCache(log, tempDir)

	f, err := c.Writer(cachePath)
	require.NoError(t, err)

	n, err := f.Write(expectedBytes)
	require.NoError(t, err)
	require.Equal(t, len(expectedBytes), n)

	err = f.Close()
	require.NoError(t, err)

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
	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	c := NewDiskCache(log, tempDir)

	// Act, assert
	_, err = c.Reader("non/existing/path")
	require.ErrorIs(t, err, ErrNotInCache)
}
