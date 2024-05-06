package storage

import (
	"context"
	"errors"
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
	return newStorage(log, topicFactory, batcherFactory, true)
}

func NewWithAutoCreate(
	log logger.Logger,
	topicFactory func(log logger.Logger, topicName string) (*Topic, error),
	batcherFactory func(logger.Logger, *Topic) RecordBatcher,
	autoCreateTopics bool,
) *Storage {
	return newStorage(log, topicFactory, batcherFactory, autoCreateTopics)
}

func newStorage(
	log logger.Logger,
	topicFactory func(log logger.Logger, topicName string) (*Topic, error),
	batcherFactory func(logger.Logger, *Topic) RecordBatcher,
	autoCreateTopics bool,
) *Storage {
	return &Storage{
		log:              log,
		autoCreateTopics: autoCreateTopics,
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

	offset, err := tb.batcher.AddRecord(record)
	if err != nil {
		return 0, fmt.Errorf("adding batch to topic '%s': %w", topicName, err)
	}
	return offset, nil
}

func (s *Storage) GetRecord(topicName string, offset uint64) (recordbatch.Record, error) {
	tb, err := s.getTopicBatcher(topicName)
	if err != nil {
		return nil, err
	}

	return tb.topic.ReadRecord(offset)
}

// GetRecords returns records starting from startOffset and until either:
// 1) ctx is cancelled
// 2) maxRecords has been reached
// 3) softMaxBytes has been reached
//
// softMaxBytes is "soft" because it will not be honored if it means returning
// zero records. In this case, at least one record will be returned.
// NOTE: GetRecords will always return all of the records that it managed to
// fetch until one of the above conditions were met. This means that the
// returned value should be used even if err is non-nil!
func (s *Storage) GetRecords(ctx context.Context, topicName string, startOffset uint64, maxRecords int, softMaxBytes int) (recordbatch.RecordBatch, error) {
	recordBatch := make([]recordbatch.Record, 0, maxRecords)

	tb, err := s.getTopicBatcher(topicName)
	if err != nil {
		return recordBatch, err
	}

	recordBatchBytes := 0
	maxOffset := startOffset + uint64(maxRecords)
	for offset := startOffset; offset < maxOffset; offset++ {
		// stop when ctx expires
		select {
		case <-ctx.Done():
			return recordBatch, ctx.Err()
		default:
		}

		record, err := tb.topic.ReadRecord(offset)
		if err != nil {
			// no more records available
			if errors.Is(err, ErrOutOfBounds) {
				break
			}

			return recordBatch, fmt.Errorf("reading record at offset %d: %w", offset, err)
		}

		trackBytes := softMaxBytes > 0
		withinByteSize := recordBatchBytes+len(record) <= softMaxBytes
		firstRecord := len(recordBatch) == 0

		// Possibilities:
		// 1) we don't care about the size
		// 2) we care about the size but the first record is larger than the
		// given soft max. In order not to potentially block the consumer
		// indefinitely, we return at least one record.
		// 3) we care about the size and it has to be within the soft max
		if !trackBytes || firstRecord || trackBytes && withinByteSize {
			recordBatchBytes += len(record)
			recordBatch = append(recordBatch, record)
		}

		// exit loop if we reached capacity
		if trackBytes && !withinByteSize {
			break
		}
	}

	return recordBatch, nil
}

// EndOffset returns the most recent offset
func (s *Storage) EndOffset(topicName string) (uint64, error) {
	tb, err := s.getTopicBatcher(topicName)
	if err != nil {
		return 0, err
	}

	return tb.topic.EndOffset(), nil
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
