package sebrecords

import (
	"fmt"

	seb "github.com/micvbang/simple-event-broker"
)

type Batch struct {
	sizes   []uint32
	data    []byte
	indexes []int32
}

func NewBatch(recordSizes []uint32, data []byte) Batch {
	indexes := make([]int32, len(recordSizes)+1)
	index := int32(0)
	for i, recordSize := range recordSizes {
		indexes[i] = index
		index += int32(recordSize)
	}
	indexes[len(recordSizes)] = int32(len(data))

	return Batch{
		sizes:   recordSizes,
		data:    data,
		indexes: indexes,
	}
}

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

func (b Batch) Len() int {
	return len(b.sizes)
}

func (b Batch) Sizes() []uint32 {
	return b.sizes
}

func (b Batch) Data() []byte {
	return b.data
}

func (b Batch) Records(startIndex int, endIndex int) ([]byte, error) {
	if startIndex >= len(b.sizes) || endIndex > len(b.sizes) {
		return nil, seb.ErrOutOfBounds
	}

	if startIndex >= endIndex {
		return nil, fmt.Errorf("%w: start (%d) must be smaller than end (%d)", seb.ErrBadInput, startIndex, endIndex)
	}

	startByte, endByte := b.indexes[startIndex], b.indexes[endIndex]
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
