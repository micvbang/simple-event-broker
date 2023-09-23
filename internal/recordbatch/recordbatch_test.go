package recordbatch_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/micvbang/simple-commit-log/internal/recordbatch"
	"github.com/stretchr/testify/require"
)

// TestWrite verifies that Write() writes the expected data to the given
// io.Writer.
func TestWrite(t *testing.T) {
	records := [][]byte{
		[]byte("hapshapshaps"),
		[]byte("nu skal vi ha snaps!"),
	}

	expectedHeader := recordbatch.Header{
		MagicBytes: recordbatch.FileFormatMagicBytes,
		Version:    recordbatch.FileFormatVersion,
		NumRecords: uint32(len(records)),
	}
	buf := bytes.NewBuffer(nil)

	// Test
	err := recordbatch.Write(buf, records)
	require.NoError(t, err)

	// Verify
	gotHeader := recordbatch.Header{}
	err = binary.Read(buf, binary.LittleEndian, &gotHeader)

	require.NoError(t, err)
	require.Equal(t, expectedHeader, gotHeader)

	recordIndices := [2]int32{}
	err = binary.Read(buf, binary.LittleEndian, &recordIndices)
	require.NoError(t, err)

	require.EqualValues(t, 0, recordIndices[0])
	require.EqualValues(t, len(records[0]), recordIndices[1])
}

// TestReadRecord verifies that ReadRecord() returns the expected data when
// reading a specific record from a RecordBatch.
func TestReadRecord(t *testing.T) {
	records := [][]byte{
		[]byte("hapshapshaps"),
		[]byte("nu skal vi ha snaps!"),
		[]byte("ind til midt og ud til navel"),
	}

	buf := bytes.NewBuffer(nil)
	err := recordbatch.Write(buf, records)
	require.NoError(t, err)

	rdr := bytes.NewReader(buf.Bytes())
	recordBatch, err := recordbatch.Parse(rdr)
	require.NoError(t, err)

	tests := map[string]struct {
		rdr         io.ReadSeeker
		recordIndex uint32
		expected    []byte
	}{
		"first": {
			recordIndex: 0,
			expected:    records[0],
		},
		"middle": {
			recordIndex: 1,
			expected:    records[1],
		},
		"last": {
			recordIndex: 2,
			expected:    records[2],
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, err)

			// Test
			got, err := recordBatch.Record(test.recordIndex)

			// Verify
			require.NoError(t, err)
			require.Equal(t, test.expected, got)
		})
	}
}

// TestReadRecordOutOfBounds verifies that ErrOutOfBounds is returned when attempting
// to read a record that does not exist.
func TestReadRecordOutOfBounds(t *testing.T) {
	records := [][]byte{
		[]byte("hapshapshaps"),
		[]byte("nu skal vi ha snaps!"),
	}

	buf := bytes.NewBuffer(nil)
	err := recordbatch.Write(buf, records)
	require.NoError(t, err)

	rdr := bytes.NewReader(buf.Bytes())
	recordBatch, err := recordbatch.Parse(rdr)
	require.NoError(t, err)

	// Test
	_, err = recordBatch.Record(2)

	// Verify
	require.ErrorIs(t, err, recordbatch.ErrOutOfBounds)
}
