package httphandlers

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"mime/multipart"
	"net/http"
	"time"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
)

type RecordsGetter interface {
	GetRecordBatch(ctx context.Context, topicName string, offset uint64, maxRecords int, softMaxBytes int) (recordbatch.RecordBatch, error)
}

const multipartFormData = "multipart/form-data"

func GetRecords(log logger.Logger, s RecordsGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Debugf("hit %s", r.URL)

		mediatype, _, err := mime.ParseMediaType(r.Header.Get("Accept"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotAcceptable)
			return
		}
		if mediatype != "*/*" && mediatype != multipartFormData {
			http.Error(w, fmt.Sprintf("set Accept: %s", multipartFormData), http.StatusMultipleChoices)
			return
		}

		ctx := r.Context()
		var cancel func()

		qparams := []QParam{
			{Key: topicNameKey, Parser: QueryString},
			{Key: offsetKey, Parser: QueryUint64},
			{Key: softMaxBytesKey, Parser: QueryIntDefault(0)},
			{Key: maxRecordsKey, Parser: QueryIntDefault(10)},
			{Key: timeoutKey, Parser: QueryDurationDefault(0)},
		}
		params, err := parseQueryParams(r, qparams...)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			log.Errorf("parsing url params: %s", err)
			fmt.Fprintf(w, "parsing url params: %s", err)
			return
		}

		topicName := params[topicNameKey].(string)
		offset := params[offsetKey].(uint64)
		softMaxBytes := params[softMaxBytesKey].(int)
		maxRecords := params[maxRecordsKey].(int)
		timeout := params[timeoutKey].(time.Duration)
		if timeout != 0 {
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		log = log.
			WithField("topic-name", topicName).
			WithField("offset", offset).
			WithField("soft-max-bytes", softMaxBytes).
			WithField("max-records", maxRecords).
			WithField("timeout", timeout)

		recordBatch, err := s.GetRecordBatch(ctx, topicName, offset, maxRecords, softMaxBytes)
		if err != nil {
			if errors.Is(err, seb.ErrTopicNotFound) {
				log.Debugf("not found: %s", err)
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w, "topic not found")
				return
			}

			if errors.Is(err, seb.ErrOutOfBounds) {
				log.Debugf("offset out of bounds: %s", err)
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w, "offset out of bounds")
				return
			}

			log.Errorf("reading record: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to read record '%d': %s", offset, err)
		}

		mw := multipart.NewWriter(w)
		w.Header().Set("Content-Type", mw.FormDataContentType())
		for localOffset, record := range recordBatch {
			fw, err := mw.CreateFormField(fmt.Sprintf("%d", offset+uint64(localOffset)))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if _, err := fw.Write(record); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		if err := mw.Close(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}
