package topic_test

import (
	"io"
	"path"
	"testing"

	"github.com/micvbang/go-helpy/slicey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/topic"
	"github.com/stretchr/testify/require"
)

// TestMemoryTopicStorageSimpleReadWrite verifies that Writer returns an
// io.WriteCloser that saves data for Reader to read it back.
func TestMemoryTopicStorageSimpleReadWrite(t *testing.T) {
	const recordBatchPath = "topicName/000123.record_batch"
	expectedBytes := tester.RandomBytes(t, 64)

	memoryStorage := topic.NewMemoryStorage(log)

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
// seb.ErrNotInStorage when attempting to read a file that does not exist.
func TestMemoryTopicStorageReadNotFound(t *testing.T) {
	memoryStorage := topic.NewMemoryStorage(log)

	_, err := memoryStorage.Reader("does-not-exist")
	require.ErrorIs(t, err, seb.ErrNotInStorage)
}

// TestMemoryTopicStorageMultiReadWrite verifies that multiple files can be
// written and read multiple times.
func TestMemoryTopicStorageMultiReadWrite(t *testing.T) {
	memoryStorage := topic.NewMemoryStorage(log)

	keys := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}

	for i := 0; i < 1000; i++ {
		key := slicey.Random(keys)
		expected := tester.RandomBytes(t, 32)

		// Act
		writeFile(t, memoryStorage, key, expected)

		rdr, err := memoryStorage.Reader(key)
		require.NoError(t, err)

		// Assert
		got := tester.ReadAndClose(t, rdr)
		require.Equal(t, expected, got)
	}
}

// TestMemoryTopicStorageListFiles verifies that ListFiles respects the
// topic name prefix when listing files, i.e. only returns files from
// the given topic.
func TestMemoryTopicStorageListFiles(t *testing.T) {
	memoryStorage := topic.NewMemoryStorage(log)

	bs := tester.RandomBytes(t, 32)

	const (
		topicName1 = "topic-name-1"
		topicName2 = "topic-name-2"
	)

	writeFile(t, memoryStorage, path.Join(topicName1, "1.ext"), bs)
	writeFile(t, memoryStorage, path.Join(topicName1, "2.ext"), bs)
	writeFile(t, memoryStorage, path.Join(topicName2, "3.ext"), bs)

	// topic1
	topic1Files, err := memoryStorage.ListFiles(topicName1, ".ext")
	require.NoError(t, err)
	require.Equal(t, 2, len(topic1Files))

	// topic2
	topic2Files, err := memoryStorage.ListFiles(topicName2, ".ext")
	require.NoError(t, err)
	require.Equal(t, 1, len(topic2Files))

	// non-existing topic
	nonExistingTopicFiles, err := memoryStorage.ListFiles("does-not-exist", ".ext")
	require.NoError(t, err)
	require.Equal(t, 0, len(nonExistingTopicFiles))
}

func writeFile(t *testing.T, ms *topic.MemoryTopicStorage, key string, bs []byte) {
	wtr, err := ms.Writer(key)
	require.NoError(t, err)

	tester.WriteAndClose(t, wtr, bs)
}
