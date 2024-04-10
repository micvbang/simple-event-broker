package storage_test

import (
	"io"
	"testing"

	"github.com/micvbang/go-helpy/slicey"
	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

// TestMemoryTopicStorageSimpleReadWrite verifies that Writer returns an
// io.WriteCloser that saves data for Reader to read it back.
func TestMemoryTopicStorageSimpleReadWrite(t *testing.T) {
	const recordBatchPath = "topicName/000123.record_batch"
	expectedBytes := tester.RandomBytes(t, 64)

	memoryStorage := storage.NewMemoryTopicStorage(log)

	// Write
	{
		// Act
		wtr, err := memoryStorage.Writer(recordBatchPath)
		require.NoError(t, err)

		n, err := wtr.Write(expectedBytes)
		require.NoError(t, err)
		require.Equal(t, len(expectedBytes), n)

		// Assert
		err = wtr.Close()
		require.NoError(t, err)
	}

	// Read
	{
		// Act
		rdr, err := memoryStorage.Reader(recordBatchPath)
		require.NoError(t, err)

		// Assert
		gotBytes, err := io.ReadAll(rdr)
		require.NoError(t, err)

		require.Equal(t, expectedBytes, gotBytes)
	}
}

// TestMemoryTopicStorageReadNotFound verifies that Reader returns
// storage.ErrNotInStorage when attempting to read a file that does not exist.
func TestMemoryTopicStorageReadNotFound(t *testing.T) {
	memoryStorage := storage.NewMemoryTopicStorage(log)

	_, err := memoryStorage.Reader("does-not-exist")
	require.ErrorIs(t, err, storage.ErrNotInStorage)
}

// TestMemoryTopicStorageMultiReadWrite verifies that multiple files can be
// written and read multiple times.
func TestMemoryTopicStorageMultiReadWrite(t *testing.T) {
	memoryStorage := storage.NewMemoryTopicStorage(log)

	keys := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}

	for i := 0; i < 1000; i++ {
		key := slicey.Random(keys)
		expected := tester.RandomBytes(t, 32)

		// Act
		wtr, err := memoryStorage.Writer(key)
		require.NoError(t, err)
		tester.WriteAndClose(t, wtr, expected)

		rdr, err := memoryStorage.Reader(key)
		require.NoError(t, err)

		// Assert
		got := tester.ReadAndClose(t, rdr)
		require.Equal(t, expected, got)
	}
}
