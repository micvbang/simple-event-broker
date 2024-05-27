package sebtopic

import (
	"io"

	"github.com/klauspost/compress/gzip"
)

// Gzip implements the Compress interface for gzip compression.
type Gzip struct{}

var _ Compress = Gzip{}

func (Gzip) NewWriter(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}

func (Gzip) NewReader(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}
