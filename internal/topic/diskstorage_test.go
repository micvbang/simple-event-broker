package topic_test

import (
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/topic"
	"github.com/stretchr/testify/require"
)

// DiskTopic is mostly tested indirectly in topic_test.go

// TestDiskTopicWriterReaderHappyPath verifies that what's written to
// DiskStorage can also be read back.
func TestDiskTopicWriterReaderHappyPath(t *testing.T) {
	expectedBytes := tester.RandomBytes(t, 512)
	const recordBatchKey = "some-key"

	d := topic.NewDiskStorage(log, t.TempDir())

	// Act, write
	wtr, err := d.Writer(recordBatchKey)
	require.NoError(t, err)
	tester.WriteAndClose(t, wtr, expectedBytes)

	// Act, read
	rdr, err := d.Reader(recordBatchKey)
	require.NoError(t, err)

	// Assert
	gotBytes := tester.ReadAndClose(t, rdr)
	require.Equal(t, expectedBytes, gotBytes)
}
