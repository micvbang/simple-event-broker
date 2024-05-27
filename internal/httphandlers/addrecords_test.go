package httphandlers_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/stretchr/testify/require"
)

// TestAddRecordsHappyPath verifies that http.StatusCreated is returned for a
// valid request to POST /records.
func TestAddRecordsHappyPath(t *testing.T) {
	const topicName = "topic"

	server := tester.HTTPServer(t)
	defer server.Close()

	expectedRecords := tester.MakeRandomRecords(32)
	expectedOffsets := make([]uint64, 32)
	for i := 0; i < 32; i++ {
		expectedOffsets[i] = uint64(i)
	}

	buf := bytes.NewBuffer(nil)
	r := httptest.NewRequest("POST", "/records", buf)
	contentType := writeMultipartFormDataPayload(t, buf, expectedRecords)

	r.Header.Add("Content-Type", contentType)
	httphelpers.AddQueryParams(r, map[string]string{
		"topic-name": topicName,
	})

	// Act
	response := server.DoWithAuth(r)

	// Assert
	require.Equal(t, http.StatusCreated, response.StatusCode)
	require.Equal(t, "application/json", response.Header.Get("Content-Type"))

	output := httphandlers.AddRecordsOutput{}
	err := httphelpers.ParseJSONAndClose(response.Body, &output)
	require.NoError(t, err)
	require.Equal(t, expectedOffsets, output.Offsets)

	gotRecords, err := server.Storage.GetRecords(context.Background(), topicName, 0, len(expectedRecords), 0)
	require.NoError(t, err)

	require.Equal(t, expectedRecords, gotRecords)
}

// TestAddRecordsPayloadTooLarge verifies that http.StatusRequestEntityTooLarge
// is returned when AddRecords() receives seb.ErrPayloadTooLarge from its
// dependency.
func TestAddRecordsPayloadTooLarge(t *testing.T) {
	deps := &httphandlers.MockDependencies{}
	deps.AddRecordsMock = func(topicName string, records []recordbatch.Record) ([]uint64, error) {
		return nil, seb.ErrPayloadTooLarge
	}

	server := tester.HTTPServer(t, tester.HTTPDependencies(deps))
	defer server.Close()

	records := tester.MakeRandomRecords(1)

	buf := bytes.NewBuffer(nil)
	r := httptest.NewRequest("POST", "/records", buf)

	contentType := writeMultipartFormDataPayload(t, buf, records)
	r.Header.Add("Content-Type", contentType)
	httphelpers.AddQueryParams(r, map[string]string{
		"topic-name": "topic",
	})

	// Act
	response := server.DoWithAuth(r)

	// Assert
	require.Equal(t, http.StatusRequestEntityTooLarge, response.StatusCode)
}

// Verifies that http.BadRequest is returned when leaving out the required
// topic-name query parameter.
func TestAddRecordsMissingTopic(t *testing.T) {
	r := httptest.NewRequest("POST", "/records", bytes.NewReader(nil))
	httphelpers.AddQueryParams(r, map[string]string{
		// NOTE: no topic-name set
	})

	server := tester.HTTPServer(t)
	defer server.Close()

	response := server.DoWithAuth(r)
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
}

func writeMultipartFormDataPayload(t *testing.T, w io.Writer, records []recordbatch.Record) string {
	mw := multipart.NewWriter(w)
	for i, record := range records {
		fw, err := mw.CreateFormField(fmt.Sprintf("some-name-%d", i))
		require.NoError(t, err)

		_, err = fw.Write(record)
		require.NoError(t, err)
	}
	err := mw.Close()
	require.NoError(t, err)

	return mw.FormDataContentType()
}
