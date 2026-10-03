package httphandlers_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/stretchr/testify/require"
)

func TestHandlersRejectUnsafeTopicNames(t *testing.T) {
	server := tester.HTTPServer(t)
	defer server.Close()

	for _, endpoint := range []string{"/records", "/record", "/topic"} {
		t.Run(endpoint, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, endpoint+"?topic-name=orders%2Fchild&offset=0", nil)
			req.Header.Set("Accept", "multipart/form-data")
			res := server.DoWithAuth(req)
			defer res.Body.Close()

			require.Equal(t, http.StatusBadRequest, res.StatusCode)
		})
	}

	var wire bytes.Buffer
	ct, err := httphelpers.RecordsToMultipartFormData(&wire, []uint32{1}, []byte("x"))
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/records?topic-name=orders%2Fchild", &wire)
	req.Header.Set("Content-Type", ct)
	res := server.DoWithAuth(req)
	defer res.Body.Close()

	require.Equal(t, http.StatusBadRequest, res.StatusCode)
}
