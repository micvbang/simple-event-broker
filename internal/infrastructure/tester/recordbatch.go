package tester

import (
	"testing"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/stringy"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/stretchr/testify/require"
)

func MakeRandomRecords(size int) []recordbatch.Record {
	expectedRecordBatch := make([]recordbatch.Record, size)
	for i := 0; i < len(expectedRecordBatch); i++ {
		expectedRecordBatch[i] = recordbatch.Record(stringy.RandomN(1 + inty.RandomN(50)))
	}
	return expectedRecordBatch
}

func MakeRandomRecordsSize(records int, recordSize int) []recordbatch.Record {
	expectedRecordBatch := make([]recordbatch.Record, records)
	for i := 0; i < len(expectedRecordBatch); i++ {
		expectedRecordBatch[i] = recordbatch.Record(stringy.RandomN(recordSize))
	}
	return expectedRecordBatch
}

func RequireOffsets(t *testing.T, start uint64, stop uint64, offsets []uint64) {
	for offset := start; offset < stop; offset++ {
		require.Equal(t, offset, offsets[offset-start])
	}
}
