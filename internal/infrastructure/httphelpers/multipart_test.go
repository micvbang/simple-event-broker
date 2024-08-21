package httphelpers_test

import (
	"bytes"
	"io"
	"mime/multipart"
	"testing"

	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/go-helpy/syncy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/seberr"
	"github.com/stretchr/testify/require"
)

type multipartToRecordsTestcase struct {
	rdr         io.Reader
	boundary    string
	expectedErr error
	expected    sebrecords.Batch
}

// TestMultipartFormDataToRecordsPartsRequired verifies that
// MultipartFormDataToRecords returns expected errors when the given
// multipart/form-data encoded payload is not valid
func TestMultipartFormDataToRecordsPartsRequired(t *testing.T) {
	bufPool := syncy.NewPool(func() *bytes.Buffer {
		return bytes.NewBuffer(make([]byte, 5*sizey.MB))
	})

	tests := map[string]multipartToRecordsTestcase{
		"both nil":        recordsToMultipartFormData(t, nil, nil, sebrecords.NewBatch(nil, nil), seberr.ErrBadInput),
		"sizes nil":       recordsToMultipartFormData(t, nil, []byte("12345"), sebrecords.NewBatch(nil, nil), seberr.ErrBadInput),
		"data nil":        recordsToMultipartFormData(t, []uint32{1, 2, 3, 4, 5}, nil, sebrecords.NewBatch(nil, nil), seberr.ErrBadInput),
		"both set":        recordsToMultipartFormData(t, []uint32{2, 4, 2}, []byte("42133742"), tester.RecordsToBatch([][]byte{[]byte("42"), []byte("1337"), []byte("42")}), nil),
		"sizes too large": recordsToMultipartFormData(t, []uint32{1, 5}, []byte("12345"), sebrecords.NewBatch(nil, nil), seberr.ErrBadInput),
		"sizes too small": recordsToMultipartFormData(t, []uint32{1, 1}, []byte("12345"), sebrecords.NewBatch(nil, nil), seberr.ErrBadInput),
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := httphelpers.MultipartFormDataToRecords(test.rdr, bufPool, test.boundary)

			require.ErrorIs(t, err, test.expectedErr)
			require.Equal(t, test.expected, got)
		})
	}
}

func recordsToMultipartFormData(t testing.TB, recordSizes []uint32, recordsData []byte, expectedBatch sebrecords.Batch, expectedErr error) multipartToRecordsTestcase {
	buf := bytes.NewBuffer(nil)

	mw := multipart.NewWriter(buf)
	defer mw.Close()

	err := httphelpers.RecordsToMultipartFormDataHTTP(mw, recordSizes, recordsData)
	require.NoError(t, err)

	return multipartToRecordsTestcase{
		rdr:         buf,
		boundary:    mw.Boundary(),
		expectedErr: expectedErr,
		expected:    expectedBatch,
	}
}
