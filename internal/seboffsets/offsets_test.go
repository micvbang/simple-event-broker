package seboffsets_test

import (
	"io"
	"testing"

	"github.com/micvbang/go-helpy/bytey"
	"github.com/micvbang/go-helpy/uint64y"
	"github.com/micvbang/simple-event-broker/internal/seboffsets"
	"github.com/micvbang/simple-event-broker/seberr"
	"github.com/stretchr/testify/require"
)

// TestHappyPath verifies that a Write can write data which Parse can read back.
func TestHappyPath(t *testing.T) {
	expectedOffsets := make([]uint64, 257)
	for i := range expectedOffsets {
		expectedOffsets[i] = uint64y.Random()
	}

	buf := bytey.NewBuffer(nil)

	// Act - write
	err := seboffsets.Write(buf, expectedOffsets)
	require.NoError(t, err)

	// Prepare buffer to be read again
	_, err = buf.Seek(0, io.SeekStart)
	require.NoError(t, err)

	// Act - read
	gotOffsets, err := seboffsets.Parse(buf)
	require.NoError(t, err)

	// Assert
	require.NoError(t, err)
	require.Equal(t, expectedOffsets, gotOffsets)
}

// TestWriteReadInvalidMagicBytes verifies that Parse returns seberr.ErrBadInput
// when parsing invalid magic bytes.
func TestWriteReadInvalidMagicBytes(t *testing.T) {
	bs := make([]byte, 256)
	buf := bytey.NewBuffer(bs)

	err := seboffsets.Write(buf, []uint64{1, 2, 3})
	require.NoError(t, err)

	bs[0] = 'N'
	bs[1] = 'O'
	bs[2] = '!'

	buf = bytey.NewBuffer(bs)

	// Prepare buffer to be read again
	_, err = buf.Seek(0, io.SeekStart)
	require.NoError(t, err)

	// Act - read
	_, err = seboffsets.Parse(buf)

	// Assert
	require.ErrorIs(t, err, seberr.ErrBadInput)
}
