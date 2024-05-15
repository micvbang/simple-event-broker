package httphandlers_test

import (
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/go-helpy/uint64y"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/stretchr/testify/require"
)

// TestGetRecordsExistence verifies that http.StatusNotFound is returned when
// either the topic name does not exist or the offset is out of bounds.
func TestGetRecordsExistence(t *testing.T) {
	server := tester.HTTPServer(t, tester.HTTPStorageAutoCreateTopic(false))

	const topicName = "topicName"

	err := server.Storage.CreateTopic(topicName)
	require.NoError(t, err)

	records := tester.MakeRandomRecordBatch(16)
	for _, record := range records {
		_, err := server.Storage.AddRecord(topicName, record)
		require.NoError(t, err)
	}

	tests := map[string]struct {
		offset     uint64
		topicName  string
		statusCode int
	}{
		"record not found": {
			offset:     42,
			topicName:  topicName,
			statusCode: http.StatusPartialContent,
		},
		"topic not found": {
			offset:     0,
			topicName:  "does-not-exist",
			statusCode: http.StatusNotFound,
		},
		"found": {
			offset:     0,
			topicName:  topicName,
			statusCode: http.StatusOK,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/records", nil)
			r.Header.Add("Accept", "multipart/form-data")
			httphelpers.AddQueryParams(r, map[string]string{
				"topic-name": test.topicName,
				"offset":     fmt.Sprintf("%d", test.offset),
				"timeout":    "10ms",
			})

			// Act
			response := server.DoWithAuth(r)

			// Assert
			require.Equal(t, test.statusCode, response.StatusCode)
		})
	}
}

// TestGetRecordsURLParameters verifies that query parameters are handled as
// expected; some are required, some have defaults, and some refer to entities
// that must exist (topic name, offset).
func TestGetRecordsURLParameters(t *testing.T) {
	const topicName = "topic-name"
	server := tester.HTTPServer(t, tester.HTTPStorageAutoCreateTopic(false))

	err := server.Storage.CreateTopic(topicName)
	require.NoError(t, err)

	_, err = server.Storage.AddRecord(topicName, recordbatch.Record("record"))
	require.NoError(t, err)

	tests := map[string]struct {
		params     map[string]any
		statusCode int
	}{
		"no parameters": {
			params:     map[string]any{},
			statusCode: http.StatusBadRequest,
		},
		"all parameters": {
			params: map[string]any{
				"topic-name":  topicName,
				"offset":      0,
				"max-bytes":   1,
				"max-records": 2,
				"timeout":     "10ms",
			},
			statusCode: http.StatusOK,
		},
		"missing topic-name": {
			params: map[string]any{
				// "topic-name":  topicName,
				"offset":      0,
				"max-bytes":   1,
				"max-records": 2,
				"timeout":     "10ms",
			},
			statusCode: http.StatusBadRequest,
		},
		"missing offset": {
			params: map[string]any{
				"topic-name": topicName,
				// "offset":      0,
				"max-bytes":   1,
				"max-records": 2,
				"timeout":     "10ms",
			},
			statusCode: http.StatusBadRequest,
		},
		"missing max-bytes": {
			params: map[string]any{
				"topic-name": topicName,
				"offset":     0,
				// "max-bytes":   1,
				"max-records": 2,
				"timeout":     "10ms",
			},
			statusCode: http.StatusOK,
		},
		"missing max-records": {
			params: map[string]any{
				"topic-name": topicName,
				"offset":     0,
				"max-bytes":  1,
				// "max-records": 2,
				"timeout": "10ms",
			},
			statusCode: http.StatusOK,
		},
		"missing timeout": {
			params: map[string]any{
				"topic-name":  topicName,
				"offset":      0,
				"max-bytes":   1,
				"max-records": 2,
				// "timeout": "10ms",
			},
			statusCode: http.StatusOK,
		},
		"offset out of bounds": {
			params: map[string]any{
				"topic-name":  topicName,
				"offset":      10, // NOTE: offset does not exist
				"max-bytes":   1,
				"max-records": 2,
				"timeout":     "10ms",
			},
			statusCode: http.StatusPartialContent,
		},
		"topic-name not found": {
			params: map[string]any{
				"topic-name":  "does-not-exist",
				"offset":      0,
				"max-bytes":   1,
				"max-records": 2,
				"timeout":     "10ms",
			},
			statusCode: http.StatusNotFound,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/records", nil)
			r.Header.Add("Accept", "multipart/form-data")

			params := map[string]string{}
			for key, val := range test.params {
				params[key] = fmt.Sprintf("%v", val)
			}
			httphelpers.AddQueryParams(r, params)

			// Act
			response := server.DoWithAuth(r)

			// Assert
			require.Equal(t, test.statusCode, response.StatusCode)
		})
	}
}

// TestGetRecordsMultipartFormData verifies that the expected records are
// returned in multipart/form-data formatting.
func TestGetRecordsMultipartFormData(t *testing.T) {
	server := tester.HTTPServer(t)

	const (
		topicName  = "topicName"
		recordSize = 32
	)

	expectedRecords := make(recordbatch.RecordBatch, 16)
	for i := range len(expectedRecords) {
		expectedRecords[i] = tester.RandomBytes(t, recordSize)
		_, err := server.Storage.AddRecord(topicName, expectedRecords[i])
		require.NoError(t, err)
	}

	tests := map[string]struct {
		offset          uint64
		maxRecords      int
		maxBytes        int
		expectedRecords []recordbatch.Record
	}{
		"all records": {
			offset:          0,
			maxRecords:      len(expectedRecords),
			maxBytes:        999999999999,
			expectedRecords: expectedRecords,
		},
		"offset 8-16": {
			offset:          8,
			maxRecords:      len(expectedRecords),
			maxBytes:        999999999999,
			expectedRecords: expectedRecords[8:],
		},
		"offset 15": {
			offset:          15,
			maxRecords:      len(expectedRecords),
			maxBytes:        999999999999,
			expectedRecords: expectedRecords[15:],
		},
		"max-records 0-5": {
			offset:          0,
			maxRecords:      5,
			maxBytes:        999999999999,
			expectedRecords: expectedRecords[:5],
		},
		"max-bytes 0-5": {
			offset:          0,
			maxRecords:      len(expectedRecords),
			maxBytes:        recordSize * 5,
			expectedRecords: expectedRecords[:5],
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/records", nil)
			r.Header.Add("Accept", "multipart/form-data")
			httphelpers.AddQueryParams(r, map[string]string{
				"topic-name":  topicName,
				"offset":      fmt.Sprintf("%d", test.offset),
				"max-records": fmt.Sprintf("%d", test.maxRecords),
				"max-bytes":   fmt.Sprintf("%d", test.maxBytes),
			})

			// Act
			response := server.DoWithAuth(r)

			// Assert
			require.Equal(t, http.StatusOK, response.StatusCode)

			// Parse multipart/form-data
			_, params, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
			mr := multipart.NewReader(response.Body, params["boundary"])

			var localOffset uint64 = 0
			for part, err := mr.NextPart(); err == nil; part, err = mr.NextPart() {
				gotOffset := uint64y.FromStringOrDefault(part.FormName(), 0)
				require.Equal(t, localOffset+test.offset, gotOffset)

				expectedRecord := test.expectedRecords[localOffset]
				gotRecord, err := io.ReadAll(part)
				require.NoError(t, err)

				require.Equal(t, expectedRecord, recordbatch.Record(gotRecord))
				localOffset += 1
			}
		})
	}
}
