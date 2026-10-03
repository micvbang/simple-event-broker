package seb_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
)

func TestWriteHTTPClientRejectsFailedWrites(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusMethodNotAllowed,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			received := false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received = true
				w.WriteHeader(status)
			}))
			defer srv.Close()

			client, err := seb.NewRecordClient(srv.URL, "key")
			if err != nil {
				t.Fatal(err)
			}

			err = client.AddRecords("topic", []uint32{1}, []byte("x"))
			if err == nil || !received {
				t.Fatalf("expected rejected write: received=%v err=%v", received, err)
			}

			t.Logf("Server rejected POST with HTTP %d; AddRecords returned an error", status)
		})
	}
}

func TestWriteHTTPClientRejectsWritesWithoutPersisting(t *testing.T) {
	for _, readonly := range []bool{false, true} {
		t.Run(fmt.Sprintf("readonly=%v", readonly), func(t *testing.T) {
			server := tester.HTTPServer(t, tester.HTTPReadOnly(readonly))
			defer server.Close()

			client, err := seb.NewRecordClient(server.Server.URL, tester.DefaultAPIKey)
			if err != nil {
				t.Fatal(err)
			}

			// A malformed declared length is rejected with 400; read-only mode rejects
			// every write with 405. Both must propagate errors and leave the topic empty.
			err = client.AddRecords("not-created", []uint32{2}, []byte("x"))
			if err == nil {
				t.Fatal("expected an error for a rejected write")
			}

			topic, err := client.GetTopic("not-created")
			if err != nil || topic.NextOffset != 0 {
				t.Fatalf("expected no persisted records, topic=%+v err=%v", topic, err)
			}

			t.Log("AddRecords returned an error and topic has zero records")
		})
	}
}
