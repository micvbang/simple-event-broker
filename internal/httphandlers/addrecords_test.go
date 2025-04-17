package httphandlers_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/go-helpy/syncy"
	"github.com/micvbang/simple-event-broker/internal/httphandlers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/seberr"
	"github.com/stretchr/testify/require"
)

// TestAddRecordsHappyPath verifies that http.StatusCreated is returned for a
// valid request to POST /records.
func TestAddRecordsHappyPath(t *testing.T) {
	const topicName = "topic"

	server := tester.HTTPServer(t)
	defer server.Close()

	inputBatch := tester.MakeRandomRecordBatch(32)

	expectedOffsets := make([]uint64, inputBatch.Len())
	for i := range expectedOffsets {
		expectedOffsets[i] = uint64(i)
	}

	buf := bytes.NewBuffer(nil)
	r := httptest.NewRequest("POST", "/records", buf)
	contentType, err := httphelpers.RecordsToMultipartFormData(buf, inputBatch.Sizes, inputBatch.Data)
	require.NoError(t, err)

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
	err = httphelpers.ParseJSONAndClose(response.Body, &output)
	require.NoError(t, err)
	require.Equal(t, expectedOffsets, output.Offsets)

	batch := tester.NewBatch(inputBatch.Len(), 4096)
	err = server.Broker.GetRecords(context.Background(), &batch, topicName, 0, inputBatch.Len(), 0)
	require.NoError(t, err)

	require.Equal(t, inputBatch, batch)
}

// TestAddRecordsPayloadTooLarge verifies that http.StatusRequestEntityTooLarge
// is returned when AddRecords() receives seberr.ErrPayloadTooLarge from its
// dependency.
func TestAddRecordsPayloadTooLarge(t *testing.T) {
	deps := &httphandlers.MockDependencies{}
	deps.AddRecordsMock = func(topicName string, batch sebrecords.Batch) ([]uint64, error) {
		return nil, seberr.ErrPayloadTooLarge
	}

	server := tester.HTTPServer(t, tester.HTTPDependencies(deps))
	defer server.Close()

	batch := tester.MakeRandomRecordBatch(1)

	buf := bytes.NewBuffer(nil)
	r := httptest.NewRequest("POST", "/records", buf)

	contentType, err := httphelpers.RecordsToMultipartFormData(buf, batch.Sizes, batch.Data)
	require.NoError(t, err)

	r.Header.Add("Content-Type", contentType)
	httphelpers.AddQueryParams(r, map[string]string{
		"topic-name": "topic",
	})

	// Act
	response := server.DoWithAuth(r)

	// Assert
	require.Equal(t, http.StatusRequestEntityTooLarge, response.StatusCode)
}

// TestAddRecordsMultipartPayloadTooLarge verifies that
// http.StatusRequestEntityTooLarge is returned when AddRecords() finds that the
// size of the allocated sebrecords.Batch which is used when parsing the input.
func TestAddRecordsMultipartPayloadTooLarge(t *testing.T) {
	const batchSizeCap = 50 * sizey.B

	server := tester.HTTPServer(t, tester.HTTPBatchPool(syncy.NewPool(func() *sebrecords.Batch {
		// NOTE: this batch limits how large an input payload is allowed to be
		batch := sebrecords.NewBatch(make([]uint32, 0, 5), make([]byte, 0, batchSizeCap))
		return &batch
	})))
	defer server.Close()

	inputBatch := tester.MakeRandomRecordBatchSize(1, batchSizeCap+1)

	buf := bytes.NewBuffer(nil)
	r := httptest.NewRequest("POST", "/records", buf)

	contentType, err := httphelpers.RecordsToMultipartFormData(buf, inputBatch.Sizes, inputBatch.Data)
	require.NoError(t, err)

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

func BenchmarkAddRecords(b *testing.B) {
	const topicName = "topic"

	server := tester.HTTPServer(b)
	defer server.Close()

	batch := tester.MakeRandomRecordBatch(32)

	buf := bytes.NewBuffer(nil)
	contentType, err := httphelpers.RecordsToMultipartFormData(buf, batch.Sizes, batch.Data)
	require.NoError(b, err)

	bs := buf.Bytes()

	b.ResetTimer()

	for range b.N {
		r := httptest.NewRequest("POST", "/records", bytes.NewBuffer(bs))
		r.Header.Add("Content-Type", contentType)
		httphelpers.AddQueryParams(r, map[string]string{
			"topic-name": topicName,
		})

		server.DoWithAuth(r)
	}

}
