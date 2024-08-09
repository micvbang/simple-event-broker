package sebrecords_test

import (
	"bytes"
	"testing"

	"github.com/micvbang/go-helpy/bytey"
	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/seberr"
	"github.com/stretchr/testify/require"
)

func TestBatchRecords(t *testing.T) {
	batch := tester.RecordsToBatch([][]byte{{1}, {2}, {3}})

	tests := map[string]struct {
		start    int
		end      int
		err      error
		expected []byte
	}{
		"start > end": {
			start: 1,
			end:   0,
			err:   seberr.ErrBadInput,
		},
		"end out of bounds": {
			start: 0,
			end:   batch.Len() + 1,
			err:   seberr.ErrOutOfBounds,
		},
		"both out of bounds": {
			start: batch.Len() + 1,
			end:   batch.Len() + 2,
			err:   seberr.ErrOutOfBounds,
		},
		"first": {
			start:    0,
			end:      1,
			expected: []byte{1},
		},
		"second": {
			start:    1,
			end:      2,
			expected: []byte{2},
		},
		"third": {
			start:    2,
			end:      3,
			expected: []byte{3},
		},
		"first two": {
			start:    0,
			end:      2,
			expected: []byte{1, 2},
		},
		"second two": {
			start:    1,
			end:      3,
			expected: []byte{2, 3},
		},
		"all": {
			start:    0,
			end:      3,
			expected: []byte{1, 2, 3},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := batch.Records(test.start, test.end)
			require.ErrorIs(t, err, test.err)

			require.Equal(t, test.expected, got)
		})
	}
}

func TestBatchIndividualRecords(t *testing.T) {
	batch := tester.RecordsToBatch([][]byte{{1}, {2}, {3}})

	tests := map[string]struct {
		start    int
		end      int
		err      error
		expected [][]byte
	}{
		"start > end": {
			start: 1,
			end:   0,
			err:   seberr.ErrBadInput,
		},
		"end out of bounds": {
			start: 0,
			end:   batch.Len() + 1,
			err:   seberr.ErrOutOfBounds,
		},
		"both out of bounds": {
			start: batch.Len() + 1,
			end:   batch.Len() + 2,
			err:   seberr.ErrOutOfBounds,
		},
		"first": {
			start:    0,
			end:      1,
			expected: [][]byte{{1}},
		},
		"second": {
			start:    1,
			end:      2,
			expected: [][]byte{{2}},
		},
		"third": {
			start:    2,
			end:      3,
			expected: [][]byte{{3}},
		},
		"first two": {
			start:    0,
			end:      2,
			expected: [][]byte{{1}, {2}},
		},
		"second two": {
			start:    1,
			end:      3,
			expected: [][]byte{{2}, {3}},
		},
		"all": {
			start:    0,
			end:      3,
			expected: [][]byte{{1}, {2}, {3}},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := batch.IndividualRecordsSubset(test.start, test.end)
			require.ErrorIs(t, err, test.err)

			require.Equal(t, test.expected, got)
		})
	}
}

// TestBatchReset verifies that Reset() correctly resets the underlying buffers,
// allowing the batch to be reused.
func TestBatchReset(t *testing.T) {
	batch := tester.MakeRandomRecordBatch(50)

	buf := bytes.NewBuffer(nil)
	err := sebrecords.Write(buf, batch)
	require.NoError(t, err)

	rdr := bytey.NewBuffer(buf.Bytes())
	parser, err := sebrecords.Parse(rdr)
	require.NoError(t, err)

	got := sebrecords.NewBatch(make([]uint32, 0, batch.Len()), make([]byte, 0, len(batch.Data)))
	for range 10000 {
		startIndex := inty.RandomN(batch.Len())
		endIndex := 1 + startIndex + inty.RandomN(batch.Len()-startIndex)
		expected := tester.BatchRecords(t, batch, startIndex, endIndex)

		// Test
		got.Reset()
		err := parser.Records(&got, uint32(startIndex), uint32(endIndex))

		// Verify
		require.NoError(t, err)

		require.Equal(t, expected, got.Data)
	}
}
