package httphandlers

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/topic"
)

type TopicGetter interface {
	Metadata(topicName string) (topic.Metadata, error)
}

type GetTopicOutput struct {
	NextOffset     uint64    `json:"next_offset"`
	LatestCommitAt time.Time `json:"latest_commit_at"`
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

		metadata, err := s.Metadata(topicName)
		if err != nil {
			if errors.Is(err, seb.ErrTopicNotFound) {
				log.Debugf("not found")
				w.WriteHeader(http.StatusNotFound)
				return
			}

			log.Errorf("reading record: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to read metadata for topic '%s': %s", topicName, err)
			return
		}

		httphelpers.WriteJSON(w, &GetTopicOutput{
			NextOffset:     metadata.NextOffset,
			LatestCommitAt: metadata.LatestCommitAt,
		})
	}
}
