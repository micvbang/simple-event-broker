package sebtopic_test

import (
	"path"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	sebtopic "github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/stretchr/testify/require"
)

// TestStorageListFilesRegressionPaths verifies that the paths returned by
// ListFiles do not contain information internal to the backing storage, but
// just the key used to write the files, e.g. for DiskStorage it should be
// "topicName/key123.ext" instead of "/full/path/topicName/key123.ext".
func TestStorageListFilesRegressionPaths(t *testing.T) {
	tester.TestBackingStorage(t, func(t *testing.T, s sebtopic.Storage) {
		const topicName = "topic-name"

		expectedPath := path.Join(topicName, "1.ext")
		writeFile(t, s, expectedPath, tester.RandomBytes(t, 32))

		// Act
		gotFiles, err := s.ListFiles(topicName, ".ext", nil)
		require.NoError(t, err)

		// Assert
		require.Equal(t, 1, len(gotFiles))
		require.Equal(t, expectedPath, gotFiles[0].Path)
	})
}

// TestStorageListFilesExtensions verifies that only paths with the expected
// extension are returned.
func TestStorageListFilesExtensions(t *testing.T) {
	tester.TestBackingStorage(t, func(t *testing.T, s sebtopic.Storage) {
		const topicName = "topic-name"

		path1 := path.Join(topicName, "1.ext1")
		path2 := path.Join(topicName, "2.ext1")
		path3 := path.Join(topicName, "3.ext2")
		path4 := path.Join(topicName, "2.ext12")
		writeFile(t, s, path1, tester.RandomBytes(t, 32))
		writeFile(t, s, path2, tester.RandomBytes(t, 32))
		writeFile(t, s, path3, tester.RandomBytes(t, 32))
		writeFile(t, s, path4, tester.RandomBytes(t, 32))

		tests := map[string]struct {
			extension string
			expected  []string
		}{
			"ext1":  {extension: ".ext1", expected: []string{path1, path2}},
			"ext2":  {extension: ".ext2", expected: []string{path3}},
			"ext12": {extension: ".ext12", expected: []string{path4}},
			"empty": {extension: "", expected: []string{}},
		}

		for name, test := range tests {
			t.Run(name, func(t *testing.T) {
				// Act
				gotFiles, err := s.ListFiles(topicName, test.extension, nil)
				require.NoError(t, err)

				require.Equal(t, len(test.expected), len(gotFiles))

				// Assert
				for i, expected := range test.expected {
					require.Equal(t, expected, gotFiles[i].Path)
				}
			})
		}
	})
}
