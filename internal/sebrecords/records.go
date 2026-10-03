package sebrecords

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/simple-event-broker/seberr"
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

// Size returns the size of the full header (including record index) in bytes
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

	indexes := make([]int32, len(batch.Sizes))
	index := int32(0)
	for i, recordSize := range batch.Sizes {
		indexes[i] = index
		index += int32(recordSize)
	}

	err = binary.Write(wtr, byteOrder, indexes)
	if err != nil {
		return fmt.Errorf("writing record indexes %v: %w", indexes, err)
	}

	err = binary.Write(wtr, byteOrder, batch.Data)
	if err != nil {
		return fmt.Errorf("writing records length %s: %w", sizey.FormatBytes(batch.Len()), err)
	}

	return nil
}

type Parser struct {
	Header      Header
	recordIndex []uint64
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

	if header.MagicBytes != FileFormatMagicBytes {
		return nil, fmt.Errorf("invalid record batch magic bytes: %x", header.MagicBytes)
	}

	if header.Version != FileFormatVersion {
		return nil, fmt.Errorf("unsupported record batch version: %d", header.Version)
	}

	// Check the index against the file before allocating from an untrusted count.
	// Header.Size uses uint32, so reject headers that cannot be represented there.
	headerSize := uint64(headerBytes) + uint64(header.NumRecords)*recordIndexSize
	fileSize, err := rdr.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seeking to end of file: %w", err)
	}

	if headerSize > math.MaxUint32 || fileSize < 0 || headerSize > uint64(fileSize) {
		return nil, fmt.Errorf("invalid record batch index: header size %d, file size %d", headerSize, fileSize)
	}

	// Account for the sentinel and the widest allocation on this platform.
	if uint64(header.NumRecords)+1 > uint64(int(^uint(0)>>1))/8 {
		return nil, fmt.Errorf("record batch index too large: %d records", header.NumRecords)
	}

	payloadSize := uint64(fileSize) - headerSize
	if header.NumRecords == 0 && payloadSize != 0 {
		return nil, fmt.Errorf("record batch with no records contains %d payload bytes", payloadSize)
	}

	if _, err := rdr.Seek(headerBytes, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to record index: %w", err)
	}

	indexes := make([]uint32, int(header.NumRecords))
	if err := binary.Read(rdr, byteOrder, indexes); err != nil {
		return nil, fmt.Errorf("reading record index: %w", err)
	}

	// The final payload boundary can exceed uint32 even though each stored
	// index and each individual record size must fit the format's uint32 fields.
	recordIndex := make([]uint64, len(indexes)+1)

	for i, index := range indexes {
		offset := uint64(index)
		if i == 0 && offset != 0 {
			return nil, fmt.Errorf("invalid first record offset: %d", offset)
		}

		if offset > payloadSize || (i > 0 && offset < recordIndex[i-1]) {
			return nil, fmt.Errorf("invalid record offset %d: %d", i, offset)
		}

		recordIndex[i] = offset
	}

	recordIndex[len(indexes)] = payloadSize

	recordSizes := make([]uint32, len(indexes))

	for i := range recordSizes {
		recordSize := recordIndex[i+1] - recordIndex[i]
		if recordSize > math.MaxUint32 {
			return nil, fmt.Errorf("record %d size exceeds uint32: %d", i, recordSize)
		}

		recordSizes[i] = uint32(recordSize)
	}

	return &Parser{
		Header:      header,
		recordIndex: recordIndex,
		rdr:         rdr,
		RecordSizes: recordSizes,
	}, nil
}

func (rb *Parser) Records(batch *Batch, recordIndexStart uint32, recordIndexEnd uint32) error {
	if recordIndexStart >= rb.Header.NumRecords {
		return fmt.Errorf("%d records available, start record index %d does not exist: %w", rb.Header.NumRecords, recordIndexStart, seberr.ErrOutOfBounds)
	}
	if recordIndexEnd > rb.Header.NumRecords {
		return fmt.Errorf("%d records available, end record index %d does not exist: %w", rb.Header.NumRecords, recordIndexEnd, seberr.ErrOutOfBounds)
	}
	if recordIndexStart >= recordIndexEnd {
		return fmt.Errorf("%w: recordIndexStart (%d) must be lower than recordIndexEnd (%d)", seberr.ErrBadInput, recordIndexStart, recordIndexEnd)
	}

	requestedRecords := int(recordIndexEnd - recordIndexStart)
	recordsLeftInBatch := cap(batch.Sizes) - len(batch.Sizes)
	if requestedRecords > recordsLeftInBatch {
		return fmt.Errorf("%w: not enough records left in buffer to satisfy read; %d required, %d left", seberr.ErrPayloadTooLarge, requestedRecords, recordsLeftInBatch)
	}

	recordOffsetStart := rb.recordIndex[recordIndexStart]
	recordOffsetEnd := rb.recordIndex[recordIndexEnd]
	requestedBytes64 := recordOffsetEnd - recordOffsetStart

	bytesLeftInBatch := cap(batch.Data) - len(batch.Data)
	if requestedBytes64 > uint64(bytesLeftInBatch) {
		return fmt.Errorf("%w: not enough bytes left in buffer to satisfy read; %d required, %d left", seberr.ErrPayloadTooLarge, requestedBytes64, bytesLeftInBatch)
	}

	requestedBytes := int(requestedBytes64)

	fileOffsetStart := int64(rb.Header.Size()) + int64(recordOffsetStart)
	_, err := rb.rdr.Seek(fileOffsetStart, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seeking for record %d/%d: %w", recordIndexStart, len(rb.recordIndex), err)
	}

	// read into backing array but don't update batch.data length yet; if the read partially
	// fails, we effectively ignore anything written beyond the length
	buf := batch.Data[len(batch.Data) : len(batch.Data)+requestedBytes]
	n, err := io.ReadFull(rb.rdr, buf)
	if err != nil {
		return fmt.Errorf("reading record indexes [%d;%d]: %w", recordIndexStart, recordIndexEnd, err)
	}
	if n != requestedBytes {
		return fmt.Errorf("reading records indexes [%d;%d]: expected to read %d, read %d", recordIndexStart, recordIndexEnd, requestedBytes, n)
	}

	// update capacity now that we know the read succeeded
	batch.Data = batch.Data[:len(batch.Data)+requestedBytes]

	batch.Sizes = append(batch.Sizes, rb.RecordSizes[recordIndexStart:recordIndexStart+uint32(requestedRecords)]...)

	return nil
}

func (rb *Parser) Close() error {
	return rb.rdr.Close()
}
