package sebtopic_test

import (
	"path"
	"path/filepath"
	"testing"

	"github.com/micvbang/go-helpy"
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

// TestDiskTopicStorageListFilesAfter verifies that ListFiles respects
// *startAfter, i.e. only returns files after *startAfter
func TestDiskTopicStorageListFilesAfter(t *testing.T) {
	diskStorage := sebtopic.NewDiskStorage(log, t.TempDir())

	bs := tester.RandomBytes(t, 32)

	const topicName = "topic-name"

	path1 := path.Join(topicName, "1.ext")
	path2 := path.Join(topicName, "2.ext")
	path3 := path.Join(topicName, "3.ext")

	writeFile(t, diskStorage, path1, bs)
	writeFile(t, diskStorage, path2, bs)
	writeFile(t, diskStorage, path3, bs)

	tests := map[string]struct {
		startAfter    *string
		expectedPaths []string
	}{
		"nil": {startAfter: nil, expectedPaths: []string{path1, path2, path3}},
		"1":   {startAfter: helpy.Pointer("1.ext"), expectedPaths: []string{path2, path3}},
		"2":   {startAfter: helpy.Pointer("2.ext"), expectedPaths: []string{path3}},
		"3":   {startAfter: helpy.Pointer("3.ext"), expectedPaths: []string{}},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Act
			gotFiles, err := diskStorage.ListFiles(topicName, ".ext", test.startAfter)
			require.NoError(t, err)

			// Assert
			require.Equal(t, len(test.expectedPaths), len(gotFiles))

			for i, expectedPath := range test.expectedPaths {
				require.Equal(t, filepath.Base(expectedPath), filepath.Base(gotFiles[i].Path))
			}
		})
	}
}
