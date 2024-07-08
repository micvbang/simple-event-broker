package httphandlers

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
)

type RecordsAdder interface {
	AddRecords(topicName string, records []sebrecords.Record) ([]uint64, error)
}

type AddRecordsOutput struct {
	Offsets []uint64 `json:"offsets"`
}

func AddRecords(log logger.Logger, s RecordsAdder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		log.Debugf("hit %s", r.URL)

		params, err := parseQueryParams(r, QParam{topicNameKey, QueryString})
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err.Error())
		}
		topicName := params[topicNameKey].(string)

		mediaType, mediaParams, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != multipartFormData {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "expected Content-Type %s", multipartFormData)
			return
		}

		records := make([]sebrecords.Record, 0, 256)

		mr := multipart.NewReader(r.Body, mediaParams["boundary"])
		for part, err := mr.NextPart(); err == nil; part, err = mr.NextPart() {
			record, err := io.ReadAll(part)
			if err != nil {
				log.Errorf("reading parts of multipart/form-data: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			part.Close()
			records = append(records, record)
		}

		offsets, err := s.AddRecords(topicName, records)
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
