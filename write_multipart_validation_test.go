package seb_test

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/go-helpy/syncy"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/seberr"
)

func TestWriteHTTPRejectsExcessPayloadAtBufferCapacity(t *testing.T) {
	var wire bytes.Buffer
	ct, err := httphelpers.RecordsToMultipartFormData(&wire, []uint32{8}, []byte("12345678EXTRA"))
	if err != nil {
		t.Fatal(err)
	}

	boundary := ct[len("multipart/form-data; boundary="):]
	batch := sebrecords.NewBatch(make([]uint32, 0, 2), make([]byte, 0, 8))
	if err := httphelpers.MultipartFormDataToRecords(&wire, boundary, &batch); !errors.Is(err, seberr.ErrBadInput) {
		t.Fatalf("expected bad input for excess payload, got %v", err)
	}
}

func TestWriteHTTPRejectsOverflowedSizes(t *testing.T) {
	var wire bytes.Buffer
	ct, err := httphelpers.RecordsToMultipartFormData(&wire, []uint32{4294967295, 2}, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}

	boundary := ct[len("multipart/form-data; boundary="):]
	batch := sebrecords.NewBatch(make([]uint32, 0, 2), make([]byte, 0, 64))
	if err := httphelpers.MultipartFormDataToRecords(&wire, boundary, &batch); !errors.Is(err, seberr.ErrPayloadTooLarge) {
		t.Fatalf("expected payload too large for overflowing sizes, got %v", err)
	}
}

func TestWriteHTTPHandlerRejectsInvalidPayload(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sizes    []uint32
		payload  string
		capacity int
	}{
		{"excess payload", []uint32{8}, "12345678EXTRA", 8},
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

			if res.StatusCode != expectedStatus {
				t.Fatalf("expected HTTP %d, got %d", expectedStatus, res.StatusCode)
			}

			client, err := seb.NewRecordClient(server.Server.URL, tester.DefaultAPIKey)
			if err != nil {
				t.Fatal(err)
			}

			topic, err := client.GetTopic("corrupt")
			if err != nil || topic.NextOffset != 0 {
				t.Fatalf("expected no persisted records, topic=%+v err=%v", topic, err)
			}
		})
	}
}
