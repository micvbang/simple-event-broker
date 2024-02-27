package tester

import (
	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/stringy"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
)

func MakeRandomRecordBatch(size int) recordbatch.RecordBatch {
	expectedRecordBatch := make(recordbatch.RecordBatch, size)
	for i := 0; i < len(expectedRecordBatch); i++ {
		expectedRecordBatch[i] = recordbatch.Record(stringy.RandomN(1 + inty.RandomN(50)))
	}
	return expectedRecordBatch
}
