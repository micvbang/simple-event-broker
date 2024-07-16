package tester

import (
	"testing"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/slicey"
	"github.com/micvbang/go-helpy/stringy"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/stretchr/testify/require"
)

func MakeRandomRecords(size int) [][]byte {
	expectedRecordBatch := make([][]byte, size)
	for i := 0; i < len(expectedRecordBatch); i++ {
		expectedRecordBatch[i] = []byte(stringy.RandomN(1 + inty.RandomN(50)))
	}
	return expectedRecordBatch
}

func MakeRandomRecordsRaw(size int) ([]uint32, []byte) {
	records := make([]sebrecords.Record, size)
	for i := 0; i < len(records); i++ {
		records[i] = sebrecords.Record(stringy.RandomN(1 + inty.RandomN(50)))
	}
	return RecordsConcat(records)
}

func MakeRandomRecordsSize(records int, recordSize int) []sebrecords.Record {
	expectedRecordBatch := make([]sebrecords.Record, records)
	for i := 0; i < len(expectedRecordBatch); i++ {
		expectedRecordBatch[i] = sebrecords.Record(stringy.RandomN(recordSize))
	}
	return expectedRecordBatch
}

func RecordsConcat(records []sebrecords.Record) ([]uint32, []byte) {
	recordsRaw := make([]byte, 0, 4096)
	recordSizes := make([]uint32, len(records))
	for i, record := range records {
		recordSizes[i] = uint32(len(record))
		recordsRaw = append(recordsRaw, record...)
	}

	return recordSizes, recordsRaw
}

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

func RecordsAsBytes(records []sebrecords.Record) [][]byte {
	return slicey.Map(records, func(r sebrecords.Record) []byte {
		return r
	})
}

func BatchRecords(t testing.TB, batch sebrecords.Batch, start int, end int) []byte {
	records, err := batch.Records(start, end)
	if err != nil {
		t.Fatalf(err.Error())
	}

	return records
}

func BatchIndividualRecords(t testing.TB, batch sebrecords.Batch, start int, end int) [][]byte {
	records, err := batch.IndividualRecords(start, end)
	if err != nil {
		t.Fatalf(err.Error())
	}

	return records
}

func BatchIndividualRecordsAsBatch(t testing.TB, batch sebrecords.Batch, start int, end int) sebrecords.Batch {
	records, err := batch.IndividualRecords(start, end)
	if err != nil {
		t.Fatalf(err.Error())
	}

	return RecordsToBatch(records)
}
