package seboffsets

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/simple-event-broker/seberr"
)

var (
	FileFormatMagicBytes = [4]byte{'s', 'e', 'b', '@'}
	byteOrder            = binary.LittleEndian
)

const (
	FileFormatVersion = 1
	headerBytes       = 32
	offsetSizeBytes   = 8
)

type Header struct {
	MagicBytes  [4]byte
	Version     int16
	UnixEpochUs int64
	NumOffsets  uint32
	Reserved    [14]byte
}

// Size returns the size of the header
func (h Header) Size() uint32 {
	return headerBytes
}

var UnixEpochUs = func() int64 {
	return time.Now().UnixMicro()
}

func Write(wtr io.Writer, offsets []uint64) error {
	header := Header{
		MagicBytes:  FileFormatMagicBytes,
		UnixEpochUs: UnixEpochUs(),
		Version:     FileFormatVersion,
		NumOffsets:  uint32(len(offsets)),
	}

	err := binary.Write(wtr, byteOrder, header)
	if err != nil {
		return fmt.Errorf("writing header: %w", err)
	}

	err = binary.Write(wtr, byteOrder, offsets)
	if err != nil {
		return fmt.Errorf("writing offsets length %s: %w", sizey.FormatBytes(len(offsets)*offsetSizeBytes), err)
	}

	return nil
}

func Parse(rdr io.ReadCloser) ([]uint64, error) {
	header := Header{}
	err := binary.Read(rdr, byteOrder, &header)
	if err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}

	if header.MagicBytes != FileFormatMagicBytes {
		return nil, fmt.Errorf("%w: invalid magic bytes '%s'", seberr.ErrBadInput, header.MagicBytes)
	}

	offsets := make([]uint64, header.NumOffsets)
	err = binary.Read(rdr, byteOrder, &offsets)
	if err != nil {
		return nil, fmt.Errorf("reading offsets: %w", err)
	}

	return offsets, nil
}
