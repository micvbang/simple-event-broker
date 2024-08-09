package sebrecords_test

import (
	"testing"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/stretchr/testify/require"
)

func TestBatchRecords(t *testing.T) {
	batch := sebrecords.BatchFromRecords([][]byte{{1}, {2}, {3}})

	tests := map[string]struct {
		start    int
		end      int
		err      error
		expected []byte
	}{
		"start > end": {
			start: 1,
			end:   0,
			err:   seb.ErrBadInput,
		},
		"end out of bounds": {
			start: 0,
			end:   batch.Len() + 1,
			err:   seb.ErrOutOfBounds,
		},
		"both out of bounds": {
			start: batch.Len() + 1,
			end:   batch.Len() + 2,
			err:   seb.ErrOutOfBounds,
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
	batch := sebrecords.BatchFromRecords([][]byte{{1}, {2}, {3}})

	tests := map[string]struct {
		start    int
		end      int
		err      error
		expected [][]byte
	}{
		"start > end": {
			start: 1,
			end:   0,
			err:   seb.ErrBadInput,
		},
		"end out of bounds": {
			start: 0,
			end:   batch.Len() + 1,
			err:   seb.ErrOutOfBounds,
		},
		"both out of bounds": {
			start: batch.Len() + 1,
			end:   batch.Len() + 2,
			err:   seb.ErrOutOfBounds,
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
			got, err := batch.IndividualRecords(test.start, test.end)
			require.ErrorIs(t, err, test.err)

			require.Equal(t, test.expected, got)
		})
	}
}

// TestBatchAppend verifies that Append correctly appends batches.
func TestBatchAppend(t *testing.T) {
	tests := map[string]struct {
		b1       sebrecords.Batch
		b2       sebrecords.Batch
		expected sebrecords.Batch
	}{
		"empty": {},
		"b1 empty": {
			b2:       sebrecords.BatchFromRecords([][]byte{{1}, {2}, {3}}),
			expected: sebrecords.BatchFromRecords([][]byte{{1}, {2}, {3}}),
		},
		"b2 empty": {
			b1:       sebrecords.BatchFromRecords([][]byte{{1}, {2}, {3}}),
			expected: sebrecords.BatchFromRecords([][]byte{{1}, {2}, {3}}),
		},
		"1-6": {
			b1:       sebrecords.BatchFromRecords([][]byte{{1}, {2}, {3}}),
			b2:       sebrecords.BatchFromRecords([][]byte{{4}, {5}, {6}}),
			expected: sebrecords.BatchFromRecords([][]byte{{1}, {2}, {3}, {4}, {5}, {6}}),
		},
		"3,4,5,1,2,3": {
			b1:       sebrecords.BatchFromRecords([][]byte{{4}, {5}, {6}}),
			b2:       sebrecords.BatchFromRecords([][]byte{{1}, {2}, {3}}),
			expected: sebrecords.BatchFromRecords([][]byte{{4}, {5}, {6}, {1}, {2}, {3}}),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			test.b1.Append(test.b2)

			require.Equal(t, test.expected.Data(), test.b1.Data())
			require.Equal(t, test.expected.Sizes(), test.b1.Sizes())
		})
	}
}
