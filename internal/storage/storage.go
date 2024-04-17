package storage

import (
	"fmt"
	"sync"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
)

type RecordBatcher interface {
	AddRecord(r recordbatch.Record) error
}

type topicBatcher struct {
	batcher RecordBatcher
	storage *TopicStorage
}

type Storage struct {
	log logger.Logger

	autoCreateTopics   bool
	createTopicStorage func(log logger.Logger, topicName string) (*TopicStorage, error)
	createBatcher      func(logger.Logger, *TopicStorage) RecordBatcher

	mu           *sync.Mutex
	topicBatcher map[string]topicBatcher
}

// New returns a Storage that utilizes the given createTopicStorage and
// createBatcher to implement the Storage interface. createTopicStorage is used
// to initialize the TopicStorage for each individual topic, and createBatcher
// is used to initialize the batching strategy used for the created
// TopicStorage.
func New(
	log logger.Logger,
	createTopicStorage func(log logger.Logger, topicName string) (*TopicStorage, error),
	createBatcher func(logger.Logger, *TopicStorage) RecordBatcher,
) *Storage {
	return &Storage{
		log:                log,
		autoCreateTopics:   true,
		createTopicStorage: createTopicStorage,
		createBatcher:      createBatcher,
		mu:                 &sync.Mutex{},
		topicBatcher:       make(map[string]topicBatcher),
	}
}

func (s *Storage) AddRecord(topicName string, record recordbatch.Record) error {
	tb, err := s.getTopicBatcher(topicName)
	if err != nil {
		return err
	}

	err = tb.batcher.AddRecord(record)
	if err != nil {
		return fmt.Errorf("adding batch to topic '%s': %w", topicName, err)
	}
	return nil
}

func (s *Storage) GetRecord(topicName string, recordID uint64) (recordbatch.Record, error) {
	tb, err := s.getTopicBatcher(topicName)
	if err != nil {
		return nil, err
	}

	return tb.storage.ReadRecord(recordID)
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
		topicStorage, err := s.createTopicStorage(topicLogger, topicName)
		if err != nil {
			return topicBatcher{}, fmt.Errorf("creating topic '%s': %w", topicName, err)
		}

		batchLogger := s.log.Name("batcher").WithField("topic-name", topicName)
		batcher := s.createBatcher(batchLogger, topicStorage)

		tb = topicBatcher{
			batcher: batcher,
			storage: topicStorage,
		}
	}
	s.topicBatcher[topicName] = tb

	return tb, nil
}
