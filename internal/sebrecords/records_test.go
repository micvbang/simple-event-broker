package sebrecords_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/micvbang/go-helpy/bytey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/stretchr/testify/require"
)

// TestWrite verifies that Write() writes the expected data to the given
// io.Writer.
func TestWrite(t *testing.T) {
	const numRecords = 5
	records := tester.MakeRandomRecords(numRecords)

	unixEpochUs := time.Now().UTC().UnixMicro()

	sebrecords.UnixEpochUs = func() int64 {
		return unixEpochUs
	}

	expectedHeader := sebrecords.Header{
		MagicBytes:  sebrecords.FileFormatMagicBytes,
		Version:     sebrecords.FileFormatVersion,
		UnixEpochUs: unixEpochUs,
		NumRecords:  uint32(len(records)),
	}
	buf := bytes.NewBuffer(nil)

	// Test
	err := sebrecords.Write(buf, records)
	require.NoError(t, err)

	// Verify
	gotHeader := sebrecords.Header{}
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
	records := tester.MakeRandomRecords(5)

	buf := bytes.NewBuffer(nil)
	err := sebrecords.Write(buf, records)
	require.NoError(t, err)

	rdr := bytey.NewBuffer(buf.Bytes())
	parser, err := sebrecords.Parse(rdr)
	require.NoError(t, err)

	tests := map[string]struct {
		rdr         io.ReadSeeker
		recordIndex uint32
		expected    sebrecords.Record
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
	records := tester.MakeRandomRecords(numRecords)

	buf := bytes.NewBuffer(nil)
	err := sebrecords.Write(buf, records)
	require.NoError(t, err)

	rdr := bytey.NewBuffer(buf.Bytes())
	parser, err := sebrecords.Parse(rdr)
	require.NoError(t, err)

	// Test
	_, err = parser.Record(numRecords)

	// Verify
	require.ErrorIs(t, err, seb.ErrOutOfBounds)
}

// TestReadRecords verifies that Records() returns the expected records when
// called with valid record start and end indexes.
func TestReadRecords(t *testing.T) {
	records := tester.MakeRandomRecords(5)

	buf := bytes.NewBuffer(nil)
	err := sebrecords.Write(buf, records)
	require.NoError(t, err)

	rdr := bytey.NewBuffer(buf.Bytes())
	parser, err := sebrecords.Parse(rdr)
	require.NoError(t, err)

	tests := map[string]struct {
		rdr              io.ReadSeeker
		recordIndexStart uint32
		recordIndexEnd   uint32
		expected         []sebrecords.Record
	}{
		"first": {
			recordIndexStart: 0,
			recordIndexEnd:   1,
			expected:         records[0:1],
		},
		"last": {
			recordIndexStart: 4,
			recordIndexEnd:   5,
			expected:         records[4:5],
		},
		"first two": {
			recordIndexStart: 0,
			recordIndexEnd:   2,
			expected:         records[0:2],
		},
		"first three": {
			recordIndexStart: 0,
			recordIndexEnd:   3,
			expected:         records[0:3],
		},
		"middle three": {
			recordIndexStart: 1,
			recordIndexEnd:   4,
			expected:         records[1:4],
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, err)

			// Test
			got, err := parser.Records(test.recordIndexStart, test.recordIndexEnd)

			// Verify
			require.NoError(t, err)
			require.Equal(t, test.expected, got)
		})
	}
}

// TestReadRecordsSingleByteRecords verifies that Records() returns the expected
// records when called with valid record start and end indexes, with single-byte
// payloads.
func TestReadRecordsSingleByteRecords(t *testing.T) {
	records := []sebrecords.Record{{1}, {2}, {3}}

	buf := bytes.NewBuffer(nil)
	err := sebrecords.Write(buf, records)
	require.NoError(t, err)

	rdr := bytey.NewBuffer(buf.Bytes())
	parser, err := sebrecords.Parse(rdr)
	require.NoError(t, err)

	tests := map[string]struct {
		rdr              io.ReadSeeker
		recordIndexStart uint32
		recordIndexEnd   uint32
		expected         []sebrecords.Record
	}{
		"first": {
			recordIndexStart: 0,
			recordIndexEnd:   1,
			expected:         records[0:1],
		},
		"last": {
			recordIndexStart: 2,
			recordIndexEnd:   3,
			expected:         records[2:3],
		},
		"first two": {
			recordIndexStart: 0,
			recordIndexEnd:   2,
			expected:         records[0:2],
		},
		"first three": {
			recordIndexStart: 0,
			recordIndexEnd:   3,
			expected:         records[0:3],
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, err)

			// Test
			got, err := parser.Records(test.recordIndexStart, test.recordIndexEnd)

			// Verify
			require.NoError(t, err)
			require.Equal(t, test.expected, got)
		})
	}
}

// TestReadRecordsOutOfBounds verifies that ErrOutOfBounds is returned when attempting
// to read a record that does not exist.
func TestReadRecordsOutOfBounds(t *testing.T) {
	const numRecords = 5
	records := tester.MakeRandomRecords(numRecords)

	buf := bytes.NewBuffer(nil)
	err := sebrecords.Write(buf, records)
	require.NoError(t, err)

	rdr := bytey.NewBuffer(buf.Bytes())
	parser, err := sebrecords.Parse(rdr)
	require.NoError(t, err)

	tests := map[string]struct {
		indexStart uint32
		indexEnd   uint32
	}{
		"start out of bounds": {
			indexStart: numRecords,
			indexEnd:   3,
		},
		"end out of bounds": {
			indexStart: 0,
			indexEnd:   numRecords + 1,
		},
		"both out of bounds": {
			indexStart: numRecords,
			indexEnd:   numRecords + 1,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Test
			_, err = parser.Records(test.indexStart, test.indexEnd)

			// Verify
			require.ErrorIs(t, err, seb.ErrOutOfBounds)
		})
	}
}

// TestReadRecordsStartIndexLargerThanEnd verifies that an error is returned
// when the given start index is larger than the end index.
func TestReadRecordsStartIndexLargerThanEnd(t *testing.T) {
	const numRecords = 5
	records := tester.MakeRandomRecords(numRecords)

	buf := bytes.NewBuffer(nil)
	err := sebrecords.Write(buf, records)
	require.NoError(t, err)

	rdr := bytey.NewBuffer(buf.Bytes())
	parser, err := sebrecords.Parse(rdr)
	require.NoError(t, err)
	// Test
	_, err = parser.Records(3, 1)

	// Verify
	require.Error(t, err)
}

// BenchmarkWrite evaluates how fast sebrecords.Write can serialzie and write a
// recordbatch to an in-memory buffer.
func BenchmarkWrite(b *testing.B) {
	benchmarkWrite(b, sebrecords.Write)
}

func benchmarkWrite(b *testing.B, f func(io.Writer, []sebrecords.Record) error) {
	type testCase struct {
		recordSize int
		records    int
	}
	tests := map[string]testCase{}
	for records := 8; records < 1024; records *= 2 {
		for recordSize := 32; recordSize < 1024; recordSize *= 2 {
			tests[fmt.Sprintf("%d, %d bytes", records, recordSize)] = testCase{
				recordSize: recordSize, records: records,
			}
		}
	}

	for name, test := range tests {
		b.Run(name, func(b *testing.B) {
			records := tester.MakeRandomRecordsSize(test.records, test.recordSize)
			buf := bytes.NewBuffer(make([]byte, len(records)*test.recordSize))

			b.ResetTimer()
			for range b.N {
				err := f(buf, records)
				if err != nil {
					b.Fatalf("unexpected error: %s", err)
				}
			}
		})

	}
}
