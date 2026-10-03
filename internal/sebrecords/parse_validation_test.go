package sebrecords_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/micvbang/go-helpy/bytey"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/seberr"
	"github.com/stretchr/testify/require"
)

func TestParseRejectsMalformedBatches(t *testing.T) {
	tests := []struct {
		name    string
		magic   [4]byte
		version int16
		count   uint32
		indexes []uint32
		data    []byte
	}{
		{"invalid magic", [4]byte{'b', 'a', 'd', '!'}, 1, 1, []uint32{0}, []byte("a")},
		{"unsupported version", sebrecords.FileFormatMagicBytes, 2, 1, []uint32{0}, []byte("a")},
		{"nonzero first offset", sebrecords.FileFormatMagicBytes, 1, 1, []uint32{1}, []byte("ab")},
		{"decreasing offsets", sebrecords.FileFormatMagicBytes, 1, 3, []uint32{0, 2, 1}, []byte("abc")},
		{"offset past payload", sebrecords.FileFormatMagicBytes, 1, 2, []uint32{0, 4}, []byte("abc")},
		{"payload without records", sebrecords.FileFormatMagicBytes, 1, 0, nil, []byte("a")},
		{"truncated index", sebrecords.FileFormatMagicBytes, 1, 2, []uint32{0}, nil},
		{"huge count in tiny file", sebrecords.FileFormatMagicBytes, 1, math.MaxUint32, nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, binary.Write(&buf, binary.LittleEndian, sebrecords.Header{
				MagicBytes: tt.magic,
				Version:    tt.version,
				NumRecords: tt.count,
			}))
			require.NoError(t, binary.Write(&buf, binary.LittleEndian, tt.indexes))

			buf.Write(tt.data)
			parser, err := sebrecords.Parse(bytey.NewBuffer(buf.Bytes()))
			require.Error(t, err)
			require.Nil(t, parser)
		})
	}
}

func TestParseAcceptsEmptyRecords(t *testing.T) {
	for _, sizes := range [][]uint32{nil, {0}, {0, 1, 0, 0}} {
		t.Run(string(rune('0'+len(sizes))), func(t *testing.T) {
			data := []byte{}
			if len(sizes) > 1 {
				data = []byte("a")
			}

			var buf bytes.Buffer
			require.NoError(t, sebrecords.Write(&buf, sebrecords.NewBatch(sizes, data)))

			parser, err := sebrecords.Parse(bytey.NewBuffer(buf.Bytes()))
			require.NoError(t, err)
			defer parser.Close()

			require.Equal(t, sizes, append([]uint32(nil), parser.RecordSizes...))

			if len(sizes) == 0 {
				return
			}

			got := sebrecords.NewBatch(make([]uint32, 0, len(sizes)), make([]byte, 0, len(data)))
			require.NoError(t, parser.Records(&got, 0, uint32(len(sizes))))
			require.Equal(t, sizes, got.Sizes)
			require.Equal(t, data, got.Data)
		})
	}
}

func TestParseRejectsOverflowingFormatLengths(t *testing.T) {
	tests := []struct {
		name     string
		count    uint32
		fileSize int64
	}{
		{"header size overflow", math.MaxUint32, 32 + int64(math.MaxUint32)*4},
		{"last record size overflow", 1, 36 + int64(math.MaxUint32) + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Create(filepath.Join(t.TempDir(), "batch"))
			require.NoError(t, err)
			defer f.Close()

			require.NoError(t, binary.Write(f, binary.LittleEndian, sebrecords.Header{
				MagicBytes: sebrecords.FileFormatMagicBytes,
				Version:    1,
				NumRecords: tt.count,
			}))
			require.NoError(t, binary.Write(f, binary.LittleEndian, uint32(0)))
			require.NoError(t, f.Truncate(tt.fileSize))

			_, err = f.Seek(0, io.SeekStart)
			require.NoError(t, err)

			parser, err := sebrecords.Parse(f)
			require.Error(t, err)
			require.Nil(t, parser)
		})
	}
}

func TestParsePreservesLargePayloadBoundaries(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "batch"))
	require.NoError(t, err)
	defer f.Close()

	require.NoError(t, binary.Write(f, binary.LittleEndian, sebrecords.Header{
		MagicBytes: sebrecords.FileFormatMagicBytes,
		Version:    1,
		NumRecords: 2,
	}))
	require.NoError(t, binary.Write(f, binary.LittleEndian, []uint32{0, math.MaxUint32}))

	_, err = f.Seek(40+int64(math.MaxUint32), io.SeekStart)
	require.NoError(t, err)

	_, err = f.Write([]byte("AB"))
	require.NoError(t, err)

	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	parser, err := sebrecords.Parse(f)
	require.NoError(t, err)
	require.Equal(t, []uint32{math.MaxUint32, 2}, parser.RecordSizes)

	got := sebrecords.NewBatch(make([]uint32, 0, 2), make([]byte, 0, 2))
	require.ErrorIs(t, parser.Records(&got, 0, 2), seberr.ErrPayloadTooLarge)
	require.Empty(t, got.Sizes)
	require.Empty(t, got.Data)
	require.NoError(t, parser.Records(&got, 1, 2))
	require.Equal(t, []byte("AB"), got.Data)
	require.Equal(t, []uint32{2}, got.Sizes)
}
