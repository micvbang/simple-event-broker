package httphandlers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/micvbang/go-helpy/sizey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
)

type RecordGetter interface {
	GetRecord(batch *sebrecords.Batch, topicName string, offset uint64) ([]byte, error)
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

		// TODO: pool
		batch := sebrecords.NewBatch(make([]uint32, 0, 8192), make([]byte, 0, 10*sizey.MB))
		record, err := s.GetRecord(&batch, topicName, offset)
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
