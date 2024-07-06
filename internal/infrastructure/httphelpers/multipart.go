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
func RecordsToMultipartFormData(w io.Writer, records [][]byte) (string, error) {
	recordSizes := make([]uint32, len(records))
	totalSize := 0
	for i, record := range records {
		recordSizes[i] = uint32(len(record))
		totalSize += len(record)
	}

	recordsRaw := make([]byte, 0, totalSize)
	for _, record := range records {
		recordsRaw = append(recordsRaw, record...)
	}

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
		n, err := fw.Write(recordsRaw)
		if err != nil {
			return "", fmt.Errorf("failed to write records: %w", err)
		}
		if n != len(recordsRaw) {
			return "", fmt.Errorf("records: expected to write %d bytes, wrote %d", len(recordsRaw), n)
		}
	}

	err := mw.Close()
	if err != nil {
		return "", fmt.Errorf("sizes: closing multipart writer: %w", err)
	}

	return mw.FormDataContentType(), nil
}
