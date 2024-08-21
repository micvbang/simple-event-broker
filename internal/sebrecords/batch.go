package sebrecords

import (
	"fmt"

	"github.com/micvbang/go-helpy/slicey"
	"github.com/micvbang/simple-event-broker/seberr"
)

type Batch struct {
	sizes []uint32
	data  []byte
}

func NewBatch(recordSizes []uint32, recordsData []byte) Batch {
	return Batch{
		sizes: recordSizes,
		data:  recordsData,
	}
}

func (b Batch) Len() int {
	return len(b.sizes)
}

func (b Batch) Sizes() []uint32 {
	return b.sizes
}

func (b Batch) Data() []byte {
	return b.data
}

func (b *Batch) Reset() {
	b.data = b.data[:0]
	b.sizes = b.sizes[:0]
}

func (b Batch) Records(startIndex int, endIndex int) ([]byte, error) {
	if startIndex >= len(b.sizes) || endIndex > len(b.sizes) {
		return nil, seberr.ErrOutOfBounds
	}

	if startIndex >= endIndex {
		return nil, fmt.Errorf("%w: start (%d) must be smaller than end (%d)", seberr.ErrBadInput, startIndex, endIndex)
	}

	startByte := slicey.Sum(b.sizes[:startIndex])
	endByte := startByte + slicey.Sum(b.sizes[startIndex:endIndex])

	return b.data[startByte:endByte], nil
}

func (b Batch) IndividualRecords() [][]byte {
	if b.Len() == 0 {
		return nil
	}

	records, err := b.IndividualRecordsSubset(0, b.Len())
	if err != nil {
		panic(fmt.Sprintf("unexpected error from individual records: %s", err))
	}

	return records
}

func (b Batch) IndividualRecordsSubset(startIndex int, endIndex int) ([][]byte, error) {
	recordsData, err := b.Records(startIndex, endIndex)
	if err != nil {
		return nil, err
	}

	records := make([][]byte, endIndex-startIndex)
	bytesUsed := uint32(0)
	for i := range records {
		size := b.sizes[startIndex+i]
		records[i] = recordsData[bytesUsed : bytesUsed+size]
		bytesUsed += size
	}
	return records, nil
}
