package httphandlers

import (
	"errors"
	"fmt"
	"net/http"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
)

type RecordGetter interface {
	GetRecord(topicName string, offset uint64) (recordbatch.Record, error)
}

func GetRecord(log logger.Logger, s RecordGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Debugf("hit %s", r.URL)

		qparams := []QParam{
			{Key: offsetKey, Parser: QueryUint64},
			{Key: topicNameKey, Parser: QueryString},
		}
		params, err := parseQueryParams(r, qparams...)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err.Error())
		}
		offset := params[offsetKey].(uint64)
		topicName := params[topicNameKey].(string)

		record, err := s.GetRecord(topicName, offset)
		if err != nil {
			if errors.Is(err, seb.ErrOutOfBounds) {
				log.Debugf("not found")
				w.WriteHeader(http.StatusNotFound)
				return
			}

			log.Errorf("reading record: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to read record '%d': %s", offset, err)
		}
		w.Write(record)
	}
}
