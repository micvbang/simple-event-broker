package httphelpers

import (
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
)

const (
	RecordsMultipartSizesKey   = "sizes"
	RecordsMultipartRecordsKey = "records"
)

// RecordsToMultipartFormData formats a slice of records as according to the
// format expected by Seb's HTTP handlers
func RecordsToMultipartFormData(w io.Writer, recordSizes []uint32, recordsData []byte) (string, error) {
	mw := multipart.NewWriter(w)

	// record metadata
	{
		fw, err := mw.CreateFormField(RecordsMultipartSizesKey)
		if err != nil {
			return "", fmt.Errorf("creating form field: %w", err)
		}

		bs, err := json.Marshal(&recordSizes)
		if err != nil {
			return "", fmt.Errorf("failed to marshal sizes: %w", err)
		}
		n, err := fw.Write(bs)
		if err != nil {
			return "", fmt.Errorf("failed to write record sizes: %w", err)
		}
		if n != len(bs) {
			return "", fmt.Errorf("sizes: expected to write %d bytes, wrote %d", len(bs), n)
		}
	}

	// record data
	{
		fw, err := mw.CreateFormField(RecordsMultipartRecordsKey)
		if err != nil {
			return "", fmt.Errorf("creating form field: %w", err)
		}
		n, err := fw.Write(recordsData)
		if err != nil {
			return "", fmt.Errorf("failed to write records: %w", err)
		}
		if n != len(recordsData) {
			return "", fmt.Errorf("records: expected to write %d bytes, wrote %d", len(recordsData), n)
		}
	}

	err := mw.Close()
	if err != nil {
		return "", fmt.Errorf("sizes: closing multipart writer: %w", err)
	}

	return mw.FormDataContentType(), nil
}
