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
