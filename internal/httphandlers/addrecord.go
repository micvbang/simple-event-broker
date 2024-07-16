package httphandlers

import (
	"fmt"
	"io"
	"net/http"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
)

type AddRecordOutput struct {
	Offset uint64 `json:"offset"`
}

func AddRecord(log logger.Logger, s RecordsAdder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		log.Debugf("hit %s", r.URL)

		params, err := parseQueryParams(r, QParam{topicNameKey, QueryString})
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err.Error())
		}
		topicName := params[topicNameKey].(string)

		bs, err := io.ReadAll(r.Body)
		if err != nil {
			log.Errorf("failed to read body: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}

		offsets, err := s.AddRecords(topicName, sebrecords.BatchFromRecords([][]byte{bs}))
		if err != nil {
			log.Errorf("failed to add: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}

		if len(offsets) != 1 {
			log.Errorf("expected 1 offset, got %d: %v", len(offsets), offsets)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = httphelpers.WriteJSONWithStatusCode(w, http.StatusCreated, AddRecordOutput{
			Offset: offsets[0],
		})
		if err != nil {
			log.Errorf("failed to write json: %s", err)
		}
	}
}
