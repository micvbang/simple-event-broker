package httphandlers

import (
	"errors"
	"fmt"
	"net/http"
)

const (
	topicNameKey = "topic-name"
	recordIDKey  = "record-id"
)

func parseQueryParams(r *http.Request, requiredParams []string) (map[string]string, error) {
	err := r.ParseForm()
	if err != nil {
		return nil, fmt.Errorf("parsing form: %s", err)
	}

	errs := []error{}
	outputs := make(map[string]string, len(requiredParams))
	for _, param := range requiredParams {
		value := r.Form.Get(param)
		if len(value) == 0 {
			errs = append(errs, fmt.Errorf("query parameter '%s' required", param))
		}

		outputs[param] = value
	}

	return outputs, errors.Join(errs...)
}
