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

// Size returns the size of the header in bytes
func (h Header) Size() uint32 {
	return headerBytes + h.NumRecords*recordIndexSize
}

var UnixEpochUs = func() int64 {
	return time.Now().UnixMicro()
}

func Write(wtr io.Writer, batch Batch) error {
	header := Header{
		MagicBytes:  FileFormatMagicBytes,
		UnixEpochUs: UnixEpochUs(),
		Version:     FileFormatVersion,
		NumRecords:  uint32(batch.Len()),
	}

	err := binary.Write(wtr, byteOrder, header)
	if err != nil {
		return fmt.Errorf("writing header: %w", err)
	}

	// NOTE: batch.indexes contains the size of the final record as well; this
	// isn't included in version 1 of the file format, so we can't write it
	// here.
	indexes := batch.indexes[:len(batch.indexes)-1]

	err = binary.Write(wtr, byteOrder, indexes)
	if err != nil {
		return fmt.Errorf("writing record indexes %v: %w", indexes, err)
	}

	err = binary.Write(wtr, byteOrder, batch.Data())
	if err != nil {
		return fmt.Errorf("writing records length %s: %w", sizey.FormatBytes(batch.Len()), err)
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

	recordIndex = append(recordIndex, uint32(fileSize)-header.Size())

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

func (rb *Parser) Records(recordIndexStart uint32, recordIndexEnd uint32) (Batch, error) {
	if recordIndexStart >= rb.Header.NumRecords {
		return Batch{}, fmt.Errorf("%d records available, start record index %d does not exist: %w", rb.Header.NumRecords, recordIndexStart, seb.ErrOutOfBounds)
	}
	if recordIndexEnd > rb.Header.NumRecords {
		return Batch{}, fmt.Errorf("%d records available, end record index %d does not exist: %w", rb.Header.NumRecords, recordIndexEnd, seb.ErrOutOfBounds)
	}
	if recordIndexStart >= recordIndexEnd {
		return Batch{}, fmt.Errorf("%w: recordIndexStart (%d) must be lower than recordIndexEnd (%d)", seb.ErrBadInput, recordIndexStart, recordIndexEnd)
	}

	recordOffsetStart := rb.recordIndex[recordIndexStart]
	recordOffsetEnd := rb.recordIndex[recordIndexEnd]

	fileOffsetStart := rb.Header.Size() + recordOffsetStart
	_, err := rb.rdr.Seek(int64(fileOffsetStart), io.SeekStart)
	if err != nil {
		return Batch{}, fmt.Errorf("seeking for record %d/%d: %w", recordIndexStart, len(rb.recordIndex), err)
	}

	size := recordOffsetEnd - recordOffsetStart
	data := make([]byte, size)
	n, err := io.ReadFull(rb.rdr, data)
	if err != nil {
		return Batch{}, fmt.Errorf("reading record indexes [%d;%d]: %w", recordIndexStart, recordIndexEnd, err)
	}

	if n != int(size) {
		return Batch{}, fmt.Errorf("reading records indexes [%d;%d]: expected to read %d, read %d", recordIndexStart, recordIndexEnd, size, n)
	}

	recordSizes := make([]uint32, recordIndexEnd-recordIndexStart)
	for i := range recordSizes {
		recordSizes[i] = rb.RecordSizes[int(recordIndexStart)+i]
	}

	return NewBatch(recordSizes, data), nil
}

func (rb *Parser) Close() error {
	return rb.rdr.Close()
}
