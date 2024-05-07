package httphandlers

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/uint64y"
)

const (
	topicNameKey    = "topic-name"
	offsetKey       = "offset"
	softMaxBytesKey = "max-bytes"
	maxRecordsKey   = "max-records"
	timeoutKey      = "timeout"
)

type QParam struct {
	Key    string
	Parser func(string) (any, error)
}

func parseQueryParams(r *http.Request, requiredParams ...QParam) (map[string]any, error) {
	errs := []error{}
	outputs := make(map[string]any, len(requiredParams))
	for _, param := range requiredParams {
		strValues := r.URL.Query()[param.Key]

		var strValue string
		if len(strValues) > 0 {
			// NOTE: ignoring any but the first value
			strValue = strValues[0]
		}

		value, err := param.Parser(strValue)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to parse query parameter '%s': %s", param.Key, err))
		}
		outputs[param.Key] = value
	}

	return outputs, errors.Join(errs...)
}

var ErrQueryParameterRequired = fmt.Errorf("required")

func QueryString(s string) (any, error) {
	if s == "" {
		return "", ErrQueryParameterRequired
	}
	return s, nil
}

func QueryUint64(s string) (any, error) {
	if s == "" {
		return "", ErrQueryParameterRequired
	}

	v, err := uint64y.FromString(s)
	if err != nil {
		return 0, fmt.Errorf("parsing '%s' as a uint64", s)
	}
	return v, nil
}

func QueryIntDefault(i int) func(string) (any, error) {
	return func(s string) (any, error) {
		v, err := inty.FromString(s)
		if err != nil {
			return i, nil
		}
		return v, nil
	}
}

func QueryDurationDefault(d time.Duration) func(string) (any, error) {
	return func(s string) (any, error) {
		v, err := time.ParseDuration(s)
		if err != nil {
			return d, nil
		}
		return v, nil
	}
}
