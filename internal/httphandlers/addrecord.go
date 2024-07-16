package httphandlers

import (
	"fmt"
	"io"
	"net/http"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

type RecordAdder interface {
	AddRecord(topicName string, record []byte) (uint64, error)
}

type AddRecordOutput struct {
	Offset uint64 `json:"offset"`
}

func AddRecord(log logger.Logger, s RecordAdder) http.HandlerFunc {
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

		offset, err := s.AddRecord(topicName, bs)
		if err != nil {
			log.Errorf("failed to add: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}

		err = httphelpers.WriteJSONWithStatusCode(w, http.StatusCreated, AddRecordOutput{
			Offset: offset,
		})
		if err != nil {
			log.Errorf("failed to write json: %s", err)
		}
	}
}
