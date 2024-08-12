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

func (b Batch) IndividualRecords(startIndex int, endIndex int) ([][]byte, error) {
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

// Append appends otherBatch to b
// func (b *Batch) Append(otherBatch Batch) {
// 	b.data = append(b.data, otherBatch.data...)
// 	b.sizes = append(b.sizes, otherBatch.sizes...)
// }

func BatchFromRecords(records [][]byte) Batch {
	totalSize := 0
	recordSizes := make([]uint32, len(records))
	for i, record := range records {
		recordSizes[i] = uint32(len(record))
		totalSize += len(record)
	}

	data := make([]byte, 0, totalSize)
	for _, record := range records {
		data = append(data, record...)
	}

	return NewBatch(recordSizes, data)
}
