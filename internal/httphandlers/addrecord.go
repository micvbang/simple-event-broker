package httphandlers

import (
	"fmt"
	"io"
	"net/http"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
)

type RecordAdder interface {
	AddRecord(topicName string, record recordbatch.Record) (uint64, error)
}

type AddRecordOutput struct {
	RecordID uint64 `json:"record_id"`
}

func AddRecord(log logger.Logger, s RecordAdder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		log.Debugf("hit %s", r.URL)

		params, err := parseQueryParams(r, []string{topicNameKey})
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err.Error())
		}

		bs, err := io.ReadAll(r.Body)
		if err != nil {
			log.Errorf("failed to read body: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}

		recordID, err := s.AddRecord(params[topicNameKey], bs)
		if err != nil {
			log.Errorf("failed to add: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}

		err = httphelpers.WriteJSONWithStatusCode(w, http.StatusCreated, AddRecordOutput{
			RecordID: recordID,
		})
		if err != nil {
			log.Errorf("failed to write json: %s", err)
		}
	}
}
