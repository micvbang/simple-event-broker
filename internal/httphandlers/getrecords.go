package httphandlers

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/micvbang/go-helpy/syncy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/seberr"
)

type RecordsGetter interface {
	GetRecords(ctx context.Context, batch *sebrecords.Batch, topicName string, offset uint64, maxRecords int, softMaxBytes int) error
}

const multipartFormData = "multipart/form-data"

func GetRecords(log logger.Logger, batchPool *syncy.Pool[*sebrecords.Batch], s RecordsGetter) http.HandlerFunc {
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
			{Key: timeoutKey, Parser: QueryDurationDefault(10 * time.Second)},
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

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()

		log = log.
			WithField("topic-name", topicName).
			WithField("offset", offset).
			WithField("soft-max-bytes", softMaxBytes).
			WithField("max-records", maxRecords).
			WithField("timeout", timeout)

		var errIsContext bool
		batch := batchPool.Get()
		batch.Reset()
		defer batchPool.Put(batch)

		err = s.GetRecords(ctx, batch, topicName, offset, maxRecords, softMaxBytes)
		if err != nil {
			if errors.Is(err, seberr.ErrTopicNotFound) {
				log.Debugf("not found: %s", err)
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w, "topic not found")
				return
			}

			if errors.Is(err, seberr.ErrOutOfBounds) {
				log.Debugf("offset out of bounds: %s", err)
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w, "offset out of bounds")
				return
			}

			errIsContext = errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
			if !errIsContext {
				log.Errorf("reading record: %s", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "failed to read record '%d': %s", offset, err)
				return
			}
		}

		mw := multipart.NewWriter(w)
		defer mw.Close()
		w.Header().Set("Content-Type", mw.FormDataContentType())

		if errIsContext {
			log.Debugf("context ended: %s", err)
			w.WriteHeader(http.StatusPartialContent)
			return
		}

		log.Debugf("sizes: %d, data: %d", len(batch.Sizes), len(batch.Data))
		// TODO: pass batch instead of sizes and data
		err = httphelpers.RecordsToMultipartFormDataHTTP(mw, batch.Sizes, batch.Data)
		if err != nil {
			log.Errorf("writing record multipart form data: %s", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}
