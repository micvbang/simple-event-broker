package httphelpers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"slices"

	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/seberr"
)

const (
	RecordsMultipartSizesKey   = "sizes"
	RecordsMultipartRecordsKey = "records"
)

// RecordsToMultipartFormData formats a slice of records as according to the
// format expected by Seb's HTTP handlers
func RecordsToMultipartFormData(w io.Writer, recordSizes []uint32, recordsData []byte) (string, error) {
	mw := multipart.NewWriter(w)
	err := recordsToMultipartFormData(mw, recordSizes, recordsData)
	if err != nil {
		return "", fmt.Errorf("writing records as multipart form data: %w", err)
	}

	err = mw.Close()
	if err != nil {
		return "", fmt.Errorf("sizes: closing multipart writer: %w", err)
	}
	return mw.FormDataContentType(), nil
}

func RecordsToMultipartFormDataHTTP(mw *multipart.Writer, recordSizes []uint32, recordsData []byte) error {
	return recordsToMultipartFormData(mw, recordSizes, recordsData)
}

func recordsToMultipartFormData(mw *multipart.Writer, recordSizes []uint32, recordsData []byte) error {
	// record metadata
	{
		fw, err := mw.CreateFormField(RecordsMultipartSizesKey)
		if err != nil {
			return fmt.Errorf("creating form field: %w", err)
		}

		bs, err := json.Marshal(&recordSizes)
		if err != nil {
			return fmt.Errorf("failed to marshal sizes: %w", err)
		}
		n, err := fw.Write(bs)
		if err != nil {
			return fmt.Errorf("failed to write record sizes: %w", err)
		}
		if n != len(bs) {
			return fmt.Errorf("sizes: expected to write %d bytes, wrote %d", len(bs), n)
		}
	}

	// record data
	{
		fw, err := mw.CreateFormField(RecordsMultipartRecordsKey)
		if err != nil {
			return fmt.Errorf("creating form field: %w", err)
		}
		n, err := fw.Write(recordsData)
		if err != nil {
			return fmt.Errorf("failed to write records: %w", err)
		}
		if n != len(recordsData) {
			return fmt.Errorf("records: expected to write %d bytes, wrote %d", len(recordsData), n)
		}
	}

	return nil
}

func MultipartFormDataToRecords(r io.Reader, boundary string, batch *sebrecords.Batch) (err error) {
	if cap(batch.Sizes) == 0 || cap(batch.Data) == 0 {
		return fmt.Errorf("%w: batch must have buffers allocated (cap(Sizes)=%d, cap(Data)=%d)", seberr.ErrBadInput, cap(batch.Sizes), cap(batch.Data))
	}
	mr := multipart.NewReader(r, boundary)

	defer func() {
		// NOTE: clears batch's data if an error is returned
		if err != nil {
			batch.Reset()
		}
	}()

	batch.Reset()

	// sizes
	{
		part, err := nextPart(mr, RecordsMultipartSizesKey)
		if err != nil {
			return err
		}

		// NOTE: in order to avoid allocations we use batch.Data as a buffer
		// here. It will be overwritten with record data below.
		batch.Data = batch.Data[:cap(batch.Data)]

		// NOTE: we don't know how much data is coming in, so we have to allow for
		// short reads
		n, err := io.ReadFull(part, batch.Data)
		shortRead := errors.Is(err, io.ErrUnexpectedEOF)
		if err != nil && !shortRead {
			return fmt.Errorf("copying '%s' of multipart/form-data: %w", RecordsMultipartSizesKey, err)
		}
		if shortRead {
			batch.Data = batch.Data[:n]
		}

		err = part.Close()
		if err != nil {
			return fmt.Errorf("closing multiwriter part: %w", err)
		}

		if slices.Equal(batch.Data, []byte("null")) {
			return fmt.Errorf("%w: null not a valid list of sizes", seberr.ErrBadInput)
		}

		err = json.Unmarshal(batch.Data, &batch.Sizes)
		if err != nil {
			return fmt.Errorf("%w: parsing batch sizes JSON: %s", seberr.ErrBadInput, err)
		}

		if len(batch.Sizes) == 0 {
			return fmt.Errorf("%w: multipart '%s' must contain a json-encoded list of record sizes", seberr.ErrBadInput, RecordsMultipartSizesKey)
		}
	}

	var sizesSum uint64

	for _, size := range batch.Sizes {
		sizesSum += uint64(size)
		if sizesSum > uint64(cap(batch.Data)) {
			return fmt.Errorf("%w: record data expected to be at least %d bytes, buffer is %d bytes", seberr.ErrPayloadTooLarge, sizesSum, cap(batch.Data))
		}
	}

	// records
	{
		part, err := nextPart(mr, RecordsMultipartRecordsKey)
		if err != nil {
			return err
		}

		buf := batch.Data[:int(sizesSum)]

		// Read exactly the declared payload, including zero-byte records.
		_, err = io.ReadFull(part, buf)
		if err != nil {
			return fmt.Errorf("%w: copying '%s' of multipart/form-data: %w", seberr.ErrBadInput, RecordsMultipartRecordsKey, err)
		}

		// ReadFull alone cannot detect excess bytes when the buffer is full.
		var extra [1]byte
		if n, err := io.ReadFull(part, extra[:]); n != 0 || !errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: records payload exceeds its declared size or has an invalid ending", seberr.ErrBadInput)
		}

		batch.Data = buf

		err = part.Close()
		if err != nil {
			return fmt.Errorf("closing multiwriter part: %w", err)
		}
	}

	return nil
}

func nextPart(mr *multipart.Reader, expectedName string) (*multipart.Part, error) {
	part, err := mr.NextPart()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: %w", seberr.ErrBadInput, err)
		}

		return nil, fmt.Errorf("reading multipart form data: %w", err)
	}

	if part.FormName() != expectedName {
		return nil, fmt.Errorf("%w: expect part '%s' to be first", seberr.ErrBadInput, RecordsMultipartSizesKey)
	}

	return part, nil
}
