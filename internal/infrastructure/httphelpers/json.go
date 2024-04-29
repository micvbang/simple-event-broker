package httphelpers

import (
	"encoding/json"
	"io"
	"net/http"
)

// WriteJSON JSON marshals v and writes the result to w.
func WriteJSON(w http.ResponseWriter, v interface{}) error {
	bs, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return WriteJSONRaw(w, bs)
}

// WriteJSONWithStatusCode sets statusCode and writes JSON marshalled v to w.
func WriteJSONWithStatusCode(w http.ResponseWriter, statusCode int, v any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	return WriteJSON(w, v)
}

// WriteJSONRaw writes raw json ([]byte) to w.
func WriteJSONRaw(w http.ResponseWriter, bs []byte) error {
	w.Header().Set("Content-Type", "application/json")

	if _, err := w.Write(bs); err != nil {
		return err
	}

	return nil
}

// ParseJSONAndClose reads the body of r and unmarshals it from JSON into v
// before closing it.
func ParseJSONAndClose(r io.ReadCloser, v interface{}) error {
	defer r.Close()
	buf, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	return json.Unmarshal(buf, v)
}
