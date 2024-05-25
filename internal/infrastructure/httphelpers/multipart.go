package httphelpers

import (
	"fmt"
	"io"
	"mime/multipart"
)

func RecordsToMultipartFormData(w io.Writer, records [][]byte) (string, error) {
	mw := multipart.NewWriter(w)
	for i, record := range records {
		fw, err := mw.CreateFormField(fmt.Sprintf("some-name-%d", i))
		if err != nil {
			return "", fmt.Errorf("creating form field: %w", err)
		}

		_, err = fw.Write(record)
		if err != nil {
			return "", fmt.Errorf("writing record: %w", err)
		}
	}
	err := mw.Close()
	if err != nil {
		return "", fmt.Errorf("closing multipart writer: %w", err)
	}

	return mw.FormDataContentType(), nil
}
