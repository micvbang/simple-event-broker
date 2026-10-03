package httphelpers_test

import (
	"bytes"
	"io"
	"mime/multipart"
	"testing"

	"github.com/micvbang/go-helpy/sizey"
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

	tests := map[string]multipartToRecordsTestcase{
		"both nil":        recordsToMultipartFormData(t, nil, nil, sebrecords.Batch{}, seberr.ErrBadInput),
		"sizes nil":       recordsToMultipartFormData(t, nil, []byte("12345"), sebrecords.Batch{}, seberr.ErrBadInput),
		"data nil":        recordsToMultipartFormData(t, []uint32{1, 2, 3, 4, 5}, nil, sebrecords.Batch{}, seberr.ErrBadInput),
		"both set":        recordsToMultipartFormData(t, []uint32{2, 4, 2}, []byte("42133742"), tester.RecordsToBatch([][]byte{[]byte("42"), []byte("1337"), []byte("42")}), nil),
		"sizes too large": recordsToMultipartFormData(t, []uint32{1, 5}, []byte("12345"), sebrecords.Batch{}, seberr.ErrBadInput),
		"sizes too small": recordsToMultipartFormData(t, []uint32{1, 1}, []byte("12345"), sebrecords.Batch{}, seberr.ErrBadInput),
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			batch := sebrecords.NewBatch(make([]uint32, 0, 64), make([]byte, 0, sizey.MB))
			err := httphelpers.MultipartFormDataToRecords(test.rdr, test.boundary, &batch)

			require.ErrorIs(t, err, test.expectedErr)
			require.Equal(t, test.expected, batch)
		})
	}
}

func TestMultipartFormDataToRecordsPayloadBounds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sizes    []uint32
		data     string
		capacity int
		err      error
	}{
		{"full buffer", []uint32{8}, "12345678", 8, nil},
		{"excess at capacity", []uint32{8}, "12345678EXTRA", 8, seberr.ErrBadInput},
		{"overflowed sizes", []uint32{4294967295, 2}, "x", 64, seberr.ErrPayloadTooLarge},
		{"short payload", []uint32{8}, "1234567", 8, seberr.ErrBadInput},
		{"empty record", []uint32{0}, "", 8, nil},
		{"two empty records", []uint32{0, 0}, "", 8, nil},
		{"empty and nonempty", []uint32{0, 1}, "x", 8, nil},
		{"undeclared data", []uint32{0}, "x", 8, seberr.ErrBadInput},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wire bytes.Buffer
			mw := multipart.NewWriter(&wire)
			require.NoError(t, httphelpers.RecordsToMultipartFormDataHTTP(mw, tc.sizes, []byte(tc.data)))
			require.NoError(t, mw.Close())

			batch := sebrecords.NewBatch(make([]uint32, 0, 4), make([]byte, 0, tc.capacity))
			err := httphelpers.MultipartFormDataToRecords(&wire, mw.Boundary(), &batch)
			require.ErrorIs(t, err, tc.err)

			if tc.err == nil {
				require.Equal(t, tc.sizes, batch.Sizes)
				require.Equal(t, tc.data, string(batch.Data))
			} else {
				require.Empty(t, batch.Sizes)
				require.Empty(t, batch.Data)
			}
		})
	}
}

func recordsToMultipartFormData(t testing.TB, recordSizes []uint32, recordsData []byte, expectedBatch sebrecords.Batch, expectedErr error) multipartToRecordsTestcase {
	buf := bytes.NewBuffer(nil)

	mw := multipart.NewWriter(buf)
	defer mw.Close()

	err := httphelpers.RecordsToMultipartFormDataHTTP(mw, recordSizes, recordsData)
	require.NoError(t, err)

	if expectedBatch.Sizes == nil {
		expectedBatch.Sizes = []uint32{}
	}
	if expectedBatch.Data == nil {
		expectedBatch.Data = []byte{}
	}

	return multipartToRecordsTestcase{
		rdr:         buf,
		boundary:    mw.Boundary(),
		expectedErr: expectedErr,
		expected:    expectedBatch,
	}
}
