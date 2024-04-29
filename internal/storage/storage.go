package storage

import (
	"fmt"
	"sync"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
)

type RecordBatcher interface {
	AddRecord(r recordbatch.Record) (uint64, error)
}

type topicBatcher struct {
	batcher RecordBatcher
	topic   *Topic
}

type Storage struct {
	log logger.Logger

	autoCreateTopics bool
	topicFactory     func(log logger.Logger, topicName string) (*Topic, error)
	batcherFactory   func(logger.Logger, *Topic) RecordBatcher

	mu           *sync.Mutex
	topicBatcher map[string]topicBatcher
}

// New returns a Storage that utilizes the given createTopic and createBatcher
// to store data in the configured backing storage of the Topic. createTopic is
// used to initialize the Topic for each individual topic, and createBatcher is
// used to initialize the batching strategy used for the created Topic.
func New(
	log logger.Logger,
	topicFactory func(log logger.Logger, topicName string) (*Topic, error),
	batcherFactory func(logger.Logger, *Topic) RecordBatcher,
) *Storage {
	return &Storage{
		log:              log,
		autoCreateTopics: true,
		topicFactory:     topicFactory,
		batcherFactory:   batcherFactory,
		mu:               &sync.Mutex{},
		topicBatcher:     make(map[string]topicBatcher),
	}
}

func (s *Storage) AddRecord(topicName string, record recordbatch.Record) (uint64, error) {
	tb, err := s.getTopicBatcher(topicName)
	if err != nil {
		return 0, err
	}

	recordID, err := tb.batcher.AddRecord(record)
	if err != nil {
		return 0, fmt.Errorf("adding batch to topic '%s': %w", topicName, err)
	}
	return recordID, nil
}

func (s *Storage) GetRecord(topicName string, recordID uint64) (recordbatch.Record, error) {
	tb, err := s.getTopicBatcher(topicName)
	if err != nil {
		return nil, err
	}

	return tb.topic.ReadRecord(recordID)
}

// EndRecordID returns the most recent record id
func (s *Storage) EndRecordID(topicName string) (uint64, error) {
	tb, err := s.getTopicBatcher(topicName)
	if err != nil {
		return 0, err
	}

	return tb.topic.EndRecordID(), nil
}

func (s *Storage) getTopicBatcher(topicName string) (topicBatcher, error) {
	log := s.log.WithField("topicName", topicName)

	s.mu.Lock()
	defer s.mu.Unlock()

	tb, ok := s.topicBatcher[topicName]
	if !ok {
		log.Debugf("creating new topic batcher")
		if !s.autoCreateTopics {
			return topicBatcher{}, fmt.Errorf("%w: '%s'", ErrTopicNotFound, topicName)
		}

		// NOTE: this could block for a long time. We're holding the lock, so
		// this is terrible.
		topicLogger := s.log.Name(fmt.Sprintf("topic storage (%s)", topicName))
		topic, err := s.topicFactory(topicLogger, topicName)
		if err != nil {
			return topicBatcher{}, fmt.Errorf("creating topic '%s': %w", topicName, err)
		}

		batchLogger := s.log.Name("batcher").WithField("topic-name", topicName)
		batcher := s.batcherFactory(batchLogger, topic)

		tb = topicBatcher{
			batcher: batcher,
			topic:   topic,
		}
		s.topicBatcher[topicName] = tb
	}

	return tb, nil
}
