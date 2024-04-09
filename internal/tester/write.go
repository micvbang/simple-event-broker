package tester

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func WriteAndClose(t *testing.T, wtr io.WriteCloser, bs []byte) {
	n, err := wtr.Write(bs)
	require.NoError(t, err)
	require.Equal(t, len(bs), n)

	err = wtr.Close()
	require.NoError(t, err)
}
