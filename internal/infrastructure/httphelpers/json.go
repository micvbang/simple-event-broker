package httphelpers

import (
	"encoding/json"
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

// WriteJSONRaw writes raw json ([]byte) to w.
func WriteJSONRaw(w http.ResponseWriter, bs []byte) error {
	w.Header().Set("Content-Type", "application/json")

	if _, err := w.Write(bs); err != nil {
		return err
	}

	return nil
}
