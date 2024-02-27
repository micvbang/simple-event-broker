package recordbatch_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

// TestWrite verifies that Write() writes the expected data to the given
// io.Writer.
func TestWrite(t *testing.T) {
	const numRecords = 5
	records := tester.MakeRandomRecordBatch(numRecords)

	unixEpochUs := time.Now().UTC().UnixMicro()

	recordbatch.UnixEpochUs = func() int64 {
		return unixEpochUs
	}

	expectedHeader := recordbatch.Header{
		MagicBytes:  recordbatch.FileFormatMagicBytes,
		Version:     recordbatch.FileFormatVersion,
		UnixEpochUs: unixEpochUs,
		NumRecords:  uint32(len(records)),
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

	recordIndices := [numRecords]int32{}
	err = binary.Read(buf, binary.LittleEndian, &recordIndices)
	require.NoError(t, err)

	expectedLength := 0
	for i := 0; i < numRecords; i++ {
		require.EqualValues(t, int32(expectedLength), recordIndices[i])
		expectedLength += len(records[i])
	}
}

// TestReadRecord verifies that ReadRecord() returns the expected data when
// reading a specific record from a Parser.
func TestReadRecord(t *testing.T) {
	records := tester.MakeRandomRecordBatch(5)

	buf := bytes.NewBuffer(nil)
	err := recordbatch.Write(buf, records)
	require.NoError(t, err)

	rdr := bytes.NewReader(buf.Bytes())
	parser, err := recordbatch.Parse(rdr)
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
			got, err := parser.Record(test.recordIndex)

			// Verify
			require.NoError(t, err)
			require.Equal(t, test.expected, got)
		})
	}
}

// TestReadRecordOutOfBounds verifies that ErrOutOfBounds is returned when attempting
// to read a record that does not exist.
func TestReadRecordOutOfBounds(t *testing.T) {
	const numRecords = 5
	records := tester.MakeRandomRecordBatch(numRecords)

	buf := bytes.NewBuffer(nil)
	err := recordbatch.Write(buf, records)
	require.NoError(t, err)

	rdr := bytes.NewReader(buf.Bytes())
	parser, err := recordbatch.Parse(rdr)
	require.NoError(t, err)

	// Test
	_, err = parser.Record(numRecords)

	// Verify
	require.ErrorIs(t, err, recordbatch.ErrOutOfBounds)
}
