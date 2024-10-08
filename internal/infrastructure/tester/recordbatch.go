package tester

import (
	"testing"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/stringy"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/stretchr/testify/require"
)

func RequireOffsets(t *testing.T, start uint64, stop uint64, offsets []uint64) {
	for offset := start; offset < stop; offset++ {
		require.Equal(t, offset, offsets[offset-start])
	}
}

func MakeRandomRecordBatch(numRecords int) sebrecords.Batch {
	records := make([][]byte, numRecords)
	for i := 0; i < len(records); i++ {
		records[i] = []byte(stringy.RandomN(1 + inty.RandomN(50)))
	}
	return RecordsToBatch(records)
}

func MakeRandomRecordBatchSize(numRecords int, recordSize int) sebrecords.Batch {
	records := make([][]byte, numRecords)
	for i := 0; i < len(records); i++ {
		records[i] = []byte(stringy.RandomN(recordSize))
	}
	return RecordsToBatch(records)
}

func RecordsToBatch(records [][]byte) sebrecords.Batch {
	data := make([]byte, 0, 4096)
	recordSizes := make([]uint32, len(records))
	for i, record := range records {
		recordSizes[i] = uint32(len(record))
		data = append(data, record...)
	}

	return sebrecords.NewBatch(recordSizes, data)
}

func BatchRecords(t testing.TB, batch sebrecords.Batch, start int, end int) []byte {
	records, err := batch.Records(start, end)
	if err != nil {
		t.Fatalf(err.Error())
	}

	return records
}

func NewBatch(numRecords int, numBytes int) sebrecords.Batch {
	return sebrecords.NewBatch(make([]uint32, 0, numRecords), make([]byte, 0, numBytes))
}
