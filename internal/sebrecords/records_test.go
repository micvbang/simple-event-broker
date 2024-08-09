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
	batch := tester.MakeRandomRecordBatch(numRecords)

	unixEpochUs := time.Now().UTC().UnixMicro()

	sebrecords.UnixEpochUs = func() int64 {
		return unixEpochUs
	}

	expectedHeader := sebrecords.Header{
		MagicBytes:  sebrecords.FileFormatMagicBytes,
		Version:     sebrecords.FileFormatVersion,
		UnixEpochUs: unixEpochUs,
		NumRecords:  uint32(batch.Len()),
	}
	buf := bytes.NewBuffer(nil)

	// Test
	err := sebrecords.Write(buf, batch)
	require.NoError(t, err)

	// Verify
	gotHeader := sebrecords.Header{}
	err = binary.Read(buf, binary.LittleEndian, &gotHeader)

	require.NoError(t, err)
	require.Equal(t, expectedHeader, gotHeader)

	recordIndices := [numRecords]int32{}
	err = binary.Read(buf, binary.LittleEndian, &recordIndices)
	require.NoError(t, err)

	expectedLength := int32(0)
	for i, recordSize := range batch.Sizes() {
		require.EqualValues(t, expectedLength, recordIndices[i])
		expectedLength += int32(recordSize)
	}
}

// TestReadRecords verifies that Records() returns the expected records when
// called with valid record start and end indexes.
func TestReadRecords(t *testing.T) {
	batch := tester.MakeRandomRecordBatch(5)

	buf := bytes.NewBuffer(nil)
	err := sebrecords.Write(buf, batch)
	require.NoError(t, err)

	rdr := bytey.NewBuffer(buf.Bytes())
	parser, err := sebrecords.Parse(rdr)
	require.NoError(t, err)

	tests := map[string]struct {
		recordIndexStart uint32
		recordIndexEnd   uint32
		expected         []byte
	}{
		"first": {
			recordIndexStart: 0,
			recordIndexEnd:   1,
			expected:         tester.BatchRecords(t, batch, 0, 1),
		},
		"last": {
			recordIndexStart: 4,
			recordIndexEnd:   5,
			expected:         tester.BatchRecords(t, batch, 4, 5),
		},
		"first two": {
			recordIndexStart: 0,
			recordIndexEnd:   2,
			expected:         tester.BatchRecords(t, batch, 0, 2),
		},
		"first three": {
			recordIndexStart: 0,
			recordIndexEnd:   3,
			expected:         tester.BatchRecords(t, batch, 0, 3),
		},
		"middle three": {
			recordIndexStart: 1,
			recordIndexEnd:   4,
			expected:         tester.BatchRecords(t, batch, 1, 4),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := sebrecords.NewBatch(make([]uint32, 0, 32), make([]byte, 0, 4*1024))

			// Test
			err := parser.Records(&got, test.recordIndexStart, test.recordIndexEnd)

			// Verify
			require.NoError(t, err)
			require.Equal(t, test.expected, got.Data())
		})
	}
}

// TestReadRecordsOverCapacity verifies that Records() returns
// seb.ErrBufferTooSmall when attempting to satisfy a request that requires more
// space than is available in either buffer.
func TestReadRecordsOverCapacity(t *testing.T) {
	batch := tester.MakeRandomRecordBatch(1)

	buf := bytes.NewBuffer(nil)
	err := sebrecords.Write(buf, batch)
	require.NoError(t, err)

	rdr := bytey.NewBuffer(buf.Bytes())
	parser, err := sebrecords.Parse(rdr)
	require.NoError(t, err)

	tests := map[string]struct {
		sizes    []uint32
		data     []byte
		expected error
	}{
		"both too small": {
			expected: seb.ErrBufferTooSmall,
		},
		"sizes too small": {
			data:     make([]byte, len(batch.Data())),
			expected: seb.ErrBufferTooSmall,
		},
		"data too small": {
			sizes:    make([]uint32, batch.Len()),
			expected: seb.ErrBufferTooSmall,
		},
		"both exactly fit": {
			data:     make([]byte, 0, len(batch.Data())),
			sizes:    make([]uint32, 0, batch.Len()),
			expected: nil,
		},
		"both generously fit": {
			data:     make([]byte, 0, 5*len(batch.Data())),
			sizes:    make([]uint32, 0, 5*batch.Len()),
			expected: nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := sebrecords.NewBatch(test.sizes, test.data)

			// Test
			err := parser.Records(&got, 0, 1)

			// Verify
			require.ErrorIs(t, err, test.expected)
		})
	}
}

// TestReadRecordsSingleByteRecords verifies that Records() returns the expected
// records when called with valid record start and end indexes, with single-byte
// payloads.
func TestReadRecordsSingleByteRecords(t *testing.T) {
	batch := sebrecords.BatchFromRecords([][]byte{{1}, {2}, {3}})

	buf := bytes.NewBuffer(nil)
	err := sebrecords.Write(buf, batch)
	require.NoError(t, err)

	rdr := bytey.NewBuffer(buf.Bytes())
	parser, err := sebrecords.Parse(rdr)
	require.NoError(t, err)

	tests := map[string]struct {
		rdr              io.ReadSeeker
		recordIndexStart uint32
		recordIndexEnd   uint32
		expected         []byte
	}{
		"first": {
			recordIndexStart: 0,
			recordIndexEnd:   1,
			expected:         tester.BatchRecords(t, batch, 0, 1),
		},
		"last": {
			recordIndexStart: 2,
			recordIndexEnd:   3,
			expected:         tester.BatchRecords(t, batch, 2, 3),
		},
		"first two": {
			recordIndexStart: 0,
			recordIndexEnd:   2,
			expected:         tester.BatchRecords(t, batch, 0, 2),
		},
		"first three": {
			recordIndexStart: 0,
			recordIndexEnd:   3,
			expected:         tester.BatchRecords(t, batch, 0, 3),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := sebrecords.NewBatch(make([]uint32, 0, 32), make([]byte, 0, 4*1024))

			// Test
			err := parser.Records(&got, test.recordIndexStart, test.recordIndexEnd)

			// Verify
			require.NoError(t, err)
			require.Equal(t, test.expected, got.Data())
		})
	}
}

// TestReadRecordsOutOfBounds verifies that ErrOutOfBounds is returned when attempting
// to read a record that does not exist.
func TestReadRecordsOutOfBounds(t *testing.T) {
	const numRecords = 5
	batch := tester.MakeRandomRecordBatch(numRecords)

	buf := bytes.NewBuffer(nil)
	err := sebrecords.Write(buf, batch)

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
			err = parser.Records(&sebrecords.Batch{}, test.indexStart, test.indexEnd)

			// Verify
			require.ErrorIs(t, err, seb.ErrOutOfBounds)
		})
	}
}

// TestReadRecordsStartIndexLargerThanEnd verifies that an error is returned
// when the given start index is larger than the end index.
func TestReadRecordsStartIndexLargerThanEnd(t *testing.T) {
	batch := tester.MakeRandomRecordBatch(5)

	buf := bytes.NewBuffer(nil)
	err := sebrecords.Write(buf, batch)
	require.NoError(t, err)

	rdr := bytey.NewBuffer(buf.Bytes())
	parser, err := sebrecords.Parse(rdr)
	require.NoError(t, err)
	// Test
	err = parser.Records(&sebrecords.Batch{}, 3, 1)

	// Verify
	require.ErrorIs(t, err, seb.ErrBadInput)
}

func BenchmarkWriteRaw(b *testing.B) {
	benchmarkWriteRaw(b, sebrecords.Write)
}

func benchmarkWriteRaw(b *testing.B, f func(io.Writer, sebrecords.Batch) error) {
	type testCase struct {
		recordSize int
		records    int
	}
	tests := map[string]testCase{}
	for records := 128; records < 2048; records *= 2 {
		for recordSize := 512; recordSize < 1024; recordSize *= 2 {
			tests[fmt.Sprintf("%d, %d bytes", records, recordSize)] = testCase{
				recordSize: recordSize, records: records,
			}
		}
	}

	for name, test := range tests {
		b.Run(name, func(b *testing.B) {
			batch := tester.MakeRandomRecordBatchSize(test.records, test.recordSize)
			buf := bytes.NewBuffer(make([]byte, batch.Len()))

			b.ResetTimer()
			for range b.N {
				err := f(buf, batch)
				if err != nil {
					b.Fatalf("unexpected error: %s", err)
				}
			}
		})

	}
}

func TestWriteRaw(t *testing.T) {
	const numRecords = 5
	batch := tester.MakeRandomRecordBatch(numRecords)

	unixEpochUs := time.Now().UTC().UnixMicro()

	sebrecords.UnixEpochUs = func() int64 {
		return unixEpochUs
	}

	expectedHeader := sebrecords.Header{
		MagicBytes:  sebrecords.FileFormatMagicBytes,
		Version:     sebrecords.FileFormatVersion,
		UnixEpochUs: unixEpochUs,
		NumRecords:  uint32(batch.Len()),
	}
	buf := bytes.NewBuffer(nil)

	// Test
	err := sebrecords.Write(buf, batch)
	require.NoError(t, err)

	// Verify
	gotHeader := sebrecords.Header{}
	err = binary.Read(buf, binary.LittleEndian, &gotHeader)

	require.NoError(t, err)
	require.Equal(t, expectedHeader, gotHeader)

	recordIndices := [numRecords]int32{}
	err = binary.Read(buf, binary.LittleEndian, &recordIndices)
	require.NoError(t, err)

	expectedIndex := 0
	for i := 0; i < numRecords; i++ {
		require.EqualValues(t, int32(expectedIndex), recordIndices[i])
		records, err := batch.Records(i, i+1)
		require.NoError(t, err)

		expectedIndex += len(records)
	}
}
