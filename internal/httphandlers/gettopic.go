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
	EndOffset(topicName string) (uint64, error)
}

type GetTopicOutput struct {
	Offset uint64 `json:"next_offset"`
}

// GetTopic returns metadata for a given topic.
func GetTopic(log logger.Logger, s TopicGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Debugf("hit %s", r.URL)

		params, err := parseQueryParams(r, QParam{topicNameKey, QueryString})
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err.Error())
		}
		topicName := params[topicNameKey].(string)

		// TODO: once we're not always autocreating topics, check if topic exists

		offset, err := s.EndOffset(topicName)
		if err != nil {
			if errors.Is(err, storage.ErrOutOfBounds) {
				log.Debugf("not found")
				w.WriteHeader(http.StatusNotFound)
				return
			}

			log.Errorf("reading record: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to read record '%d': %s", offset, err)
			return
		}

		httphelpers.WriteJSON(w, &GetTopicOutput{
			Offset: offset,
		})
	}
}
