package sebrecords

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/micvbang/go-helpy/sizey"
	seb "github.com/micvbang/simple-event-broker"
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
	return time.Now().UnixMicro()
}

type Record []byte

// Write writes a RecordBatch file to wtr, consisting of a header, a record
// index, and the given records.
func Write(wtr io.Writer, rb []Record) error {
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

	// pack records into a single byte slice so that we can write them with
	// a single call to binary.Write(). This is much faster even if the number
	// if records is low (benchmarks with 8 rows were still ~2x speed-up!)
	records := make([]byte, 0, recordIndex)
	for _, record := range rb {
		records = append(records, record...)
	}

	err = binary.Write(wtr, byteOrder, records)
	if err != nil {
		return fmt.Errorf("writing records length %s: %w", sizey.FormatBytes(len(rb)), err)
	}
	return nil
}

type Parser struct {
	Header      Header
	recordIndex []uint32
	RecordSizes []uint32
	rdr         io.ReadSeekCloser
}

// Parse reads a RecordBatch file and returns a Parser which can be used to
// read individual records.
func Parse(rdr io.ReadSeekCloser) (*Parser, error) {
	header := Header{}
	err := binary.Read(rdr, byteOrder, &header)
	if err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}

	// NOTE: we're adding the size of the final record to recordIndex below,
	// once we've figured out the total file size
	recordIndex := make([]uint32, header.NumRecords, header.NumRecords+1)
	err = binary.Read(rdr, byteOrder, &recordIndex)
	if err != nil {
		return nil, fmt.Errorf("reading record index: %w", err)
	}

	// TODO: this seek is only necessary because we don't have the size of the
	// last entry in the file.
	// In order to not make the code more complex than necessary, we compute the
	// file size once, now, when the file is opened. An alternative (and
	// probably better) solution could be to include the size of the final
	// record in the file header.
	fileSize, err := rdr.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seeking to end of file: %w", err)
	}

	recordIndex = append(recordIndex, uint32(fileSize)-(headerBytes+header.NumRecords*recordIndexSize))

	recordSizes := make([]uint32, 0, len(recordIndex)-1)
	for i := 0; i < len(recordIndex)-1; i++ {
		recordSize := recordIndex[i+1] - recordIndex[i]
		recordSizes = append(recordSizes, recordSize)
	}

	return &Parser{
		Header:      header,
		recordIndex: recordIndex,
		rdr:         rdr,
		RecordSizes: recordSizes,
	}, nil
}

func (rb *Parser) Record(recordIndex uint32) (Record, error) {
	if recordIndex >= rb.Header.NumRecords {
		return nil, fmt.Errorf("%d records available, record index %d does not exist: %w", rb.Header.NumRecords, recordIndex, seb.ErrOutOfBounds)
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
	n, err := io.ReadFull(rb.rdr, record)
	if err != nil {
		return nil, fmt.Errorf("reading record: %w", err)
	}

	if n != int(size) {
		return nil, fmt.Errorf("reading record index %d: expected to read %d, read %d", recordIndex, size, n)
	}

	return record, nil
}

func (rb *Parser) Records(recordIndexStart uint32, recordIndexEnd uint32) ([]Record, error) {
	if recordIndexStart >= rb.Header.NumRecords {
		return nil, fmt.Errorf("%d records available, start record index %d does not exist: %w", rb.Header.NumRecords, recordIndexStart, seb.ErrOutOfBounds)
	}
	if recordIndexEnd > rb.Header.NumRecords {
		return nil, fmt.Errorf("%d records available, end record index %d does not exist: %w", rb.Header.NumRecords, recordIndexEnd, seb.ErrOutOfBounds)
	}
	if recordIndexStart >= recordIndexEnd {
		return nil, fmt.Errorf("recordIndexStart must be lower than recordIndexEnd")
	}

	recordOffsetStart := rb.recordIndex[recordIndexStart]
	recordOffsetEnd := rb.recordIndex[recordIndexEnd]

	fileOffsetStart := headerBytes + rb.Header.NumRecords*recordIndexSize + recordOffsetStart
	_, err := rb.rdr.Seek(int64(fileOffsetStart), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking for record %d/%d: %w", recordIndexStart, len(rb.recordIndex), err)
	}

	size := recordOffsetEnd - recordOffsetStart
	data := make([]byte, size)
	n, err := io.ReadFull(rb.rdr, data)
	if err != nil {
		return nil, fmt.Errorf("reading record indexes [%d;%d]: %w", recordIndexStart, recordIndexEnd, err)
	}

	if n != int(size) {
		return nil, fmt.Errorf("reading records indexes [%d;%d]: expected to read %d, read %d", recordIndexStart, recordIndexEnd, size, n)
	}

	records := make([]Record, recordIndexEnd-recordIndexStart)
	recordOffset := uint32(0)
	for i := range records {
		recordSize := rb.RecordSizes[int(recordIndexStart)+i]
		records[i] = data[recordOffset : recordOffset+recordSize]
		recordOffset += recordSize
	}

	return records, nil
}

func (rb *Parser) Close() error {
	return rb.rdr.Close()
}
