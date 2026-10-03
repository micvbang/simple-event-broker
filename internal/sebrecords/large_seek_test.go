package sebrecords_test

import (
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/sebrecords"
)

// This is the exact format Write produces for records of sizes MaxUint32 and 2.
// A sparse file avoids allocating or physically writing the first record's zeros.
func TestParserLargeRecordSeekDoesNotWrap(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "large.record_batch"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h := sebrecords.Header{
		MagicBytes: sebrecords.FileFormatMagicBytes,
		Version:    sebrecords.FileFormatVersion,
		NumRecords: 2,
	}
	if err := binary.Write(f, binary.LittleEndian, h); err != nil {
		t.Fatal(err)
	}

	if err := binary.Write(f, binary.LittleEndian, []uint32{0, math.MaxUint32}); err != nil {
		t.Fatal(err)
	}

	actualSecondOffset := int64(h.Size()) + int64(math.MaxUint32)
	if _, err := f.Seek(actualSecondOffset, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	if _, err := f.Write([]byte("AB")); err != nil {
		t.Fatal(err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	p, err := sebrecords.Parse(f)
	if err != nil {
		t.Fatal(err)
	}

	b := sebrecords.NewBatch(make([]uint32, 0, 1), make([]byte, 0, 2))
	if err := p.Records(&b, 1, 2); err != nil {
		t.Fatal(err)
	}

	if string(b.Data) != "AB" {
		t.Fatalf("valid second record at file offset %d: expected AB, got %q (sizes %v)", actualSecondOffset, b.Data, b.Sizes)
	}
}
