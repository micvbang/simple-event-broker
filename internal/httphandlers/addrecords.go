package httphandlers

import (
	"errors"
	"fmt"
	"mime"
	"net/http"

	"github.com/micvbang/go-helpy/syncy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/seberr"
)

type RecordsAdder interface {
	AddRecords(topicName string, batch sebrecords.Batch) ([]uint64, error)
}

type AddRecordsOutput struct {
	Offsets []uint64 `json:"offsets"`
}

func AddRecords(log logger.Logger, bufPool *syncy.Pool[*sebrecords.Batch], s RecordsAdder) http.HandlerFunc {
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

		batch := bufPool.Get()
		defer bufPool.Put(batch)
		err = httphelpers.MultipartFormDataToRecords(r.Body, mediaParams["boundary"], batch)
		if err != nil {
			switch {
			case errors.Is(err, seberr.ErrBadInput):
				w.WriteHeader(http.StatusBadRequest)
			case errors.Is(err, seberr.ErrPayloadTooLarge):
				w.WriteHeader(http.StatusRequestEntityTooLarge)
			default:
				w.WriteHeader(http.StatusInternalServerError)
			}
			fmt.Fprint(w, err.Error())
			return
		}

		offsets, err := s.AddRecords(topicName, *batch)
		if err != nil {
			if errors.Is(err, seberr.ErrPayloadTooLarge) {
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
