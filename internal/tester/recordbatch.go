package tester

import (
	"testing"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/stringy"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/stretchr/testify/require"
)

func MakeRandomRecordBatch(size int) recordbatch.RecordBatch {
	expectedRecordBatch := make(recordbatch.RecordBatch, size)
	for i := 0; i < len(expectedRecordBatch); i++ {
		expectedRecordBatch[i] = recordbatch.Record(stringy.RandomN(1 + inty.RandomN(50)))
	}
	return expectedRecordBatch
}

func RequireRecordIDs(t *testing.T, start uint64, stop uint64, recordIDs []uint64) {
	for recordID := start; recordID < stop; recordID++ {
		require.Equal(t, recordID, recordIDs[recordID-start])
	}
}
