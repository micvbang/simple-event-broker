package httphandlers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/storage"
)

type TopicGetter interface {
	EndRecordID(topicName string) (uint64, error)
}

type GetTopicOutput struct {
	RecordID uint64 `json:"next_record_id"`
}

// GetTopic returns metadata for a given topic.
func GetTopic(log logger.Logger, s TopicGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Debugf("hit %s", r.URL)

		params, err := parseQueryParams(r, []string{topicNameKey})
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err.Error())
		}

		// TODO: once we're not always autocreating topics, check if topic exists

		recordID, err := s.EndRecordID(params[topicNameKey])
		if err != nil {
			if errors.Is(err, storage.ErrOutOfBounds) {
				log.Debugf("not found")
				w.WriteHeader(http.StatusNotFound)
				return
			}

			log.Errorf("reading record: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to read record '%d': %s", recordID, err)
		}
		httphelpers.WriteJSON(w, &GetTopicOutput{
			RecordID: recordID,
		})
	}
}
