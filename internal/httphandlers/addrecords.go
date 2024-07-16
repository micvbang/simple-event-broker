package httphandlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"

	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/go-helpy/syncy"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
)

type RecordsAdder interface {
	AddRecords(topicName string, batch sebrecords.Batch) ([]uint64, error)
}

type AddRecordsOutput struct {
	Offsets []uint64 `json:"offsets"`
}

var bufPool = syncy.NewPool(func() *bytes.Buffer {
	return bytes.NewBuffer(make([]byte, 5*sizey.MB))
})

func AddRecords(log logger.Logger, s RecordsAdder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		log.Debugf("hit %s", r.URL)

		params, err := parseQueryParams(r, QParam{topicNameKey, QueryString})
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err.Error())
			return
		}
		topicName := params[topicNameKey].(string)

		mediaType, mediaParams, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != multipartFormData {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "expected Content-Type %s", multipartFormData)
			return
		}

		var recordSizes []uint32
		var recordData []byte

		mr := multipart.NewReader(r.Body, mediaParams["boundary"])
		for part, err := mr.NextPart(); err == nil; part, err = mr.NextPart() {
			buf := bufPool.Get()
			buf.Reset()
			defer bufPool.Put(buf)

			_, err = io.Copy(buf, part)
			if err != nil {
				log.Errorf("reading parts of multipart/form-data: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			err = part.Close()
			if err != nil {
				log.Errorf("failed to close part: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			switch part.FormName() {
			case httphelpers.RecordsMultipartSizesKey:
				err = json.Unmarshal(buf.Bytes(), &recordSizes)
				if err != nil {
					log.Errorf("reading sizes: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}

			case httphelpers.RecordsMultipartRecordsKey:
				recordData = buf.Bytes()

			default:
				log.Errorf("unexpected form field %s", part.FormName())
				w.WriteHeader(http.StatusBadRequest)
				return
			}

		}

		batch := sebrecords.NewBatch(recordSizes, recordData)

		// TODO: we must verify that both sizes and records have been given

		offsets, err := s.AddRecords(topicName, batch)
		if err != nil {
			if errors.Is(err, seb.ErrPayloadTooLarge) {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}

			log.Errorf("failed to add: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}

		err = httphelpers.WriteJSONWithStatusCode(w, http.StatusCreated, AddRecordsOutput{
			Offsets: offsets,
		})
		if err != nil {
			log.Errorf("failed to write json: %s", err)
		}
	}
}
