package tester

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// WriteAndClose copies all of the bytes from bs to wtr before closing it. Any
// errors will cause the test to fail.
func WriteAndClose(t *testing.T, wtr io.WriteCloser, bs []byte) {
	n, err := wtr.Write(bs)
	require.NoError(t, err)
	require.Equal(t, len(bs), n)

	err = wtr.Close()
	require.NoError(t, err)
}

// ReadAndClose reads all the contents of rdr and closes it. Any errors will
// cause the test to fail.
func ReadAndClose(t *testing.T, rdr io.ReadCloser) []byte {
	bs, err := io.ReadAll(rdr)
	require.NoError(t, err)

	err = rdr.Close()
	require.NoError(t, err)

	return bs
}
