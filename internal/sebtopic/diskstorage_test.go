package sebtopic_test

import (
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	sebtopic "github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/stretchr/testify/require"
)

// DiskTopic is mostly tested indirectly in topic_test.go

// TestDiskTopicWriterReaderHappyPath verifies that what's written to
// DiskStorage can also be read back.
func TestDiskTopicWriterReaderHappyPath(t *testing.T) {
	expectedBytes := tester.RandomBytes(t, 512)
	const recordsKey = "some-key"

	d := sebtopic.NewDiskStorage(log, t.TempDir())

	// Act, write
	wtr, err := d.Writer(recordsKey)
	require.NoError(t, err)
	tester.WriteAndClose(t, wtr, expectedBytes)

	// Act, read
	rdr, err := d.Reader(recordsKey)
	require.NoError(t, err)

	// Assert
	gotBytes := tester.ReadAndClose(t, rdr)
	require.Equal(t, expectedBytes, gotBytes)
}
