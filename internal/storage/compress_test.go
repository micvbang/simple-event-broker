package storage_test

import (
	"io"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

var compressors = []storage.Compress{
	storage.Gzip{},
}

// TestCompressors verifies that all compressors can write random bytes and read
// the same ones back again.
func TestCompressors(t *testing.T) {
	expectedBytes := tester.RandomBytes(t, 256)

	for _, compress := range compressors {
		f := tester.TempFile(t)

		{ // NewWriter
			w, err := compress.NewWriter(f)
			require.NoError(t, err)

			n, err := w.Write(expectedBytes)
			require.NoError(t, err)
			require.Equal(t, len(expectedBytes), n)

			err = w.Close()
			require.NoError(t, err)
		}

		{ // NewReader
			_, err := f.Seek(0, io.SeekStart)
			require.NoError(t, err)

			r, err := compress.NewReader(f)
			require.NoError(t, err)

			gotBytes, err := io.ReadAll(r)
			require.NoError(t, err)
			require.Equal(t, expectedBytes, gotBytes)
		}
	}
}
