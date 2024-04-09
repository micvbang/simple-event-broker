package recordbatch

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

var (
	FileFormatMagicBytes = [4]byte{'s', 'e', 'b', '!'}
	byteOrder            = binary.LittleEndian
)

const (
	FileFormatVersion = 1
	headerBytes       = 32
	recordIndexSize   = 4
)

type Header struct {
	MagicBytes  [4]byte
	Version     int16
	UnixEpochUs int64
	NumRecords  uint32
	Reserved    [14]byte
}

var UnixEpochUs = func() int64 {
	return time.Now().UTC().UnixMicro()
}

type Record []byte
type RecordBatch []Record

// Write writes a RecordBatch file to wtr, consisting of a header, a record
// index, and the given records.
func Write(wtr io.Writer, rb RecordBatch) error {
	header := Header{
		MagicBytes:  FileFormatMagicBytes,
		UnixEpochUs: UnixEpochUs(),
		Version:     FileFormatVersion,
		NumRecords:  uint32(len(rb)),
	}

	err := binary.Write(wtr, byteOrder, header)
	if err != nil {
		return fmt.Errorf("writing header: %w", err)
	}

	recordIndexes := make([]uint32, len(rb))

	var recordIndex uint32
	for i, record := range rb {
		recordIndexes[i] = recordIndex
		recordIndex += uint32(len(record))
	}

	err = binary.Write(wtr, byteOrder, recordIndexes)
	if err != nil {
		return fmt.Errorf("writing record indexes %d: %w", recordIndex, err)
	}

	for i, record := range rb {
		err = binary.Write(wtr, byteOrder, record)
		if err != nil {
			return fmt.Errorf("writing record %d/%d: %w", i+1, len(rb), err)
		}
	}
	return nil
}

var ErrOutOfBounds = fmt.Errorf("attempting to read out of bounds record")

type Parser struct {
	Header      Header
	recordIndex []uint32
	rdr         io.ReadSeeker
}

// Parse reads a RecordBatch file and returns a Parser which can be used to
// read individual records.
func Parse(rdr io.ReadSeeker) (*Parser, error) {
	header := Header{}
	err := binary.Read(rdr, byteOrder, &header)
	if err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}

	recordIndex := make([]uint32, header.NumRecords)
	err = binary.Read(rdr, byteOrder, &recordIndex)
	if err != nil {
		return nil, fmt.Errorf("reading record index: %w", err)
	}

	return &Parser{
		Header:      header,
		recordIndex: recordIndex,
		rdr:         rdr,
	}, nil
}

func (rb *Parser) Record(recordIndex uint32) (Record, error) {
	if recordIndex >= rb.Header.NumRecords {
		return nil, fmt.Errorf("%d records available, record index %d does not exist: %w", rb.Header.NumRecords, recordIndex, ErrOutOfBounds)
	}

	recordOffset := rb.recordIndex[recordIndex]

	fileOffset := headerBytes + rb.Header.NumRecords*recordIndexSize + recordOffset
	_, err := rb.rdr.Seek(int64(fileOffset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking for record %d/%d: %w", recordIndex, len(rb.recordIndex), err)
	}

	// last record, read the remainder of the file
	if recordIndex == uint32(len(rb.recordIndex)-1) {
		return io.ReadAll(rb.rdr)
	}

	// read record bytes
	size := rb.recordIndex[recordIndex+1] - recordOffset
	record := make(Record, size)
	_, err = io.ReadFull(rb.rdr, record)
	if err != nil {
		return nil, fmt.Errorf("reading record: %w", err)
	}

	return record, nil
}
