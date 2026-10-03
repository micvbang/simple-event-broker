package seb_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/go-helpy/syncy"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/stretchr/testify/require"
)

func TestRecordClientHTTPRejectsInvalidPayload(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sizes    []uint32
		payload  string
		capacity int
	}{
		{"truncated", []uint32{8}, "12345678EXTRA", 8},
		{"overflow", []uint32{4294967295, 2}, "x", 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := syncy.NewPool(func() *sebrecords.Batch {
				batch := sebrecords.NewBatch(make([]uint32, 0, 8), make([]byte, 0, tc.capacity))

				return &batch
			})
			server := tester.HTTPServer(t, tester.HTTPBatchPool(pool))
			defer server.Close()

			var wire bytes.Buffer
			ct, err := httphelpers.RecordsToMultipartFormData(&wire, tc.sizes, []byte(tc.payload))
			if err != nil {
				t.Fatal(err)
			}

			req := httptest.NewRequest(http.MethodPost, "/records?topic-name=corrupt", &wire)
			req.Header.Set("Content-Type", ct)
			res := server.DoWithAuth(req)
			defer res.Body.Close()

			expectedStatus := http.StatusBadRequest
			if tc.name == "overflow" {
				expectedStatus = http.StatusRequestEntityTooLarge
			}

			require.Equal(t, expectedStatus, res.StatusCode)

			metadata, err := server.Broker.Metadata("corrupt")
			require.NoError(t, err)
			require.Zero(t, metadata.NextOffset)

		})
	}
}

func TestRecordClientGetRecordsEmptyRecord(t *testing.T) {
	srv := tester.HTTPServer(t)
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	err = client.AddRecords("proof", []uint32{0, 1}, []byte("x"))
	require.NoError(t, err, "the public HTTP write API accepts an empty record alongside a nonempty record")

	single, err := client.GetRecord("proof", 0)
	require.NoError(t, err)
	require.Empty(t, single, "the committed empty record can be read individually")

	records, err := client.GetRecords("proof", 0, seb.GetRecordsInput{MaxRecords: 1})
	require.NoError(t, err, "a committed record readable individually must also be readable in a batch")
	require.Len(t, records, 1)
	require.Empty(t, records[0])
}

func TestRecordClientGetRecordsPayloadFitsBuffer(t *testing.T) {
	srv := tester.HTTPServer(t)
	defer srv.Close()

	_, err := srv.Broker.AddRecords("proof", sebrecords.NewBatch([]uint32{1}, []byte("x")))
	require.NoError(t, err)

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	records, err := client.GetRecords("proof", 0, seb.GetRecordsInput{
		MaxRecords: 1,
		Buffer:     make([]byte, 1),
	})
	require.NoError(t, err, "the complete one-byte record fits the caller's buffer")
	require.Equal(t, [][]byte{[]byte("x")}, records)
}
