package httphelpers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"

	"github.com/micvbang/go-helpy/slicey"
	"github.com/micvbang/go-helpy/syncy"
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

func MultipartFormDataToRecords(r io.Reader, bufPool *syncy.Pool[*bytes.Buffer], boundary string) (sebrecords.Batch, error) {
	var batch sebrecords.Batch

	var recordSizes []uint32
	var recordData []byte

	mr := multipart.NewReader(r, boundary)
	for part, err := mr.NextPart(); err == nil; part, err = mr.NextPart() {
		buf := bufPool.Get()
		buf.Reset()
		defer bufPool.Put(buf)

		_, err = io.Copy(buf, part)
		if err != nil {
			return batch, fmt.Errorf("copying part of multipart/form-data: %w", err)
		}
		err = part.Close()
		if err != nil {
			return batch, fmt.Errorf("closing multiwriter part: %w", err)
		}

		switch part.FormName() {
		case RecordsMultipartSizesKey:
			err = json.Unmarshal(buf.Bytes(), &recordSizes)
			if err != nil {
				return batch, fmt.Errorf("%w: parsing batch sizes JSON: %s", seberr.ErrBadInput, err)
			}

		case RecordsMultipartRecordsKey:
			recordData = buf.Bytes()

		default:
			return batch, fmt.Errorf("%w: unexpected part name '%s'", seberr.ErrBadInput, part.FormName())
		}
	}

	if recordSizes == nil {
		return batch, fmt.Errorf("%w: part '%s' not given", seberr.ErrBadInput, RecordsMultipartSizesKey)
	}
	if recordData == nil {
		return batch, fmt.Errorf("%w: part '%s' not given", seberr.ErrBadInput, RecordsMultipartRecordsKey)
	}

	// Verify input sizes match input data
	sizesSum := slicey.Sum(recordSizes)
	if int(sizesSum) != len(recordData) {
		return batch, fmt.Errorf("%w: expected sizes to sum up to size of data. sizes sum: %d, records data: %d ", seberr.ErrBadInput, sizesSum, len(recordData))
	}

	return sebrecords.NewBatch(recordSizes, recordData), nil
}
