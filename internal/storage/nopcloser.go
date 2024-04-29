package storage

import "io"

type nopWriteCloser struct {
	io.Writer
}

func NopWriteCloser(w io.Writer) io.WriteCloser {
	return &nopWriteCloser{w}
}

func (nopWriteCloser) Close() error {
	return nil
}

type nopReadSeekCloser struct {
	io.ReadSeeker
}

func NopReadSeekCloser(r io.ReadSeeker) io.ReadSeekCloser {
	return &nopReadSeekCloser{r}
}

func (nopReadSeekCloser) Close() error {
	return nil
}
