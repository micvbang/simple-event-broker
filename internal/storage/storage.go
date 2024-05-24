package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/micvbang/go-helpy/sizey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/topic"
)

type RecordBatcher interface {
	AddRecord(r recordbatch.Record) (uint64, error)
}

type topicBatcher struct {
	batcher RecordBatcher
	topic   *topic.Topic
}

type Storage struct {
	log logger.Logger

	autoCreateTopics bool
	topicFactory     func(log logger.Logger, topicName string) (*topic.Topic, error)
	batcherFactory   func(logger.Logger, *topic.Topic) RecordBatcher

	mu            *sync.Mutex
	topicBatchers map[string]topicBatcher
}

type Opts struct {
	AutoCreateTopic bool
	BatcherFactory  batcherFactory
}

// New returns a Storage that utilizes topicFactory to store records.
//
// It defaults to automatically create topics if they don't already exist.
// It defaults to batch records using NewBlockingBatcherFactory(1s, 10MB),
// meaning that added records will only be persisted once one of these limits
// have been reached; 1 second has passed, or the total size of records waiting
// exceeds 10MB.
//
// If you wish to change the defaults, use the WithXX methods.
func New(log logger.Logger, topicFactory TopicFactory, optFuncs ...func(*Opts)) *Storage {
	opts := Opts{
		AutoCreateTopic: true,
		BatcherFactory:  NewBlockingBatcherFactory(1*time.Second, 10*sizey.MB),
	}

	for _, optFunc := range optFuncs {
		optFunc(&opts)
	}

	return &Storage{
		log:              log,
		autoCreateTopics: opts.AutoCreateTopic,
		topicFactory:     topicFactory,
		batcherFactory:   opts.BatcherFactory,
		mu:               &sync.Mutex{},
		topicBatchers:    make(map[string]topicBatcher),
	}
}

// AddRecord adds record to topicName, using the configured batcher. It returns
// only once data has been committed to topic storage.
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

// GetRecord returns the record at offset in topicName. It will only return offsets
// that have been committed to topic storage.
func (s *Storage) GetRecord(topicName string, offset uint64) (recordbatch.Record, error) {
	tb, err := s.getTopicBatcher(topicName)
	if err != nil {
		return nil, err
	}

	return tb.topic.ReadRecord(offset)
}

// CreateTopic creates a topic with the given name and default configuration.
func (s *Storage) CreateTopic(topicName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO: make topic configurable, e.g.
	// - compression
	// - mime type?
	// TODO: store information about topic configuration somewhere

	_, exists := s.topicBatchers[topicName]
	if exists {
		return seb.ErrTopicAlreadyExists
	}

	tb, err := s.makeTopicBatcher(topicName)
	if err != nil {
		return err
	}

	// since topicBatchers is just a local cache of the topics that were
	// instantiated during the lifetime of Storage, we don't yet know whether
	// the topic already exists or not. Checking the topic's nextOffset is a
	// hacky way to attempt to do this.
	if tb.topic.NextOffset() != 0 {
		return seb.ErrTopicAlreadyExists
	}

	s.topicBatchers[topicName] = tb
	return err
}

// GetRecords returns records starting from startOffset and until either:
// 1) ctx is cancelled
// 2) maxRecords has been reached
// 3) softMaxBytes has been reached
//
// maxRecords defaults to 10 if 0 is given.
// softMaxBytes is "soft" because it will not be honored if it means returning
// zero records. In this case, at least one record will be returned.
//
// NOTE: GetRecordBatch will always return all of the records that it managed to
// fetch until one of the above conditions were met. This means that the
// returned value should be used even if err is non-nil!
func (s *Storage) GetRecords(ctx context.Context, topicName string, offset uint64, maxRecords int, softMaxBytes int) (recordbatch.RecordBatch, error) {
	if maxRecords == 0 {
		maxRecords = 10
	}

	tb, err := s.getTopicBatcher(topicName)
	if err != nil {
		return nil, err
	}

	// wait for startOffset to become available. Can only return errors from
	// the context
	err = tb.topic.OffsetCond.Wait(ctx, offset)
	if err != nil {
		ctxExpiredErr := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		if ctxExpiredErr {
			return nil, fmt.Errorf("waiting for offset %d to be reached: %w", offset, err)
		}

		s.log.Errorf("unexpected error when waiting for offset %d to be reached: %s", offset, err)
		return nil, fmt.Errorf("unexpected when waiting for offset %d to be reached: %w", offset, err)
	}

	return tb.topic.ReadRecords(ctx, offset, maxRecords, softMaxBytes)
}

// Metadata returns metadata about the topic.
func (s *Storage) Metadata(topicName string) (topic.Metadata, error) {
	tb, err := s.getTopicBatcher(topicName)
	if err != nil {
		return topic.Metadata{}, err
	}

	return tb.topic.Metadata()
}

// makeTopicBatcher initializes a new topicBatcher, but does not put it into
// s.topicBatchers.
func (s *Storage) makeTopicBatcher(topicName string) (topicBatcher, error) {
	// NOTE: this could block for a long time. We're holding the lock, so
	// this is terrible.
	topicLogger := s.log.Name(fmt.Sprintf("topic storage (%s)", topicName))
	topic, err := s.topicFactory(topicLogger, topicName)
	if err != nil {
		return topicBatcher{}, fmt.Errorf("creating topic '%s': %w", topicName, err)
	}

	batchLogger := s.log.Name("batcher").WithField("topic-name", topicName)
	batcher := s.batcherFactory(batchLogger, topic)

	tb := topicBatcher{
		batcher: batcher,
		topic:   topic,
	}

	return tb, nil
}

func (s *Storage) getTopicBatcher(topicName string) (topicBatcher, error) {
	var err error
	log := s.log.WithField("topicName", topicName)

	s.mu.Lock()
	defer s.mu.Unlock()

	tb, ok := s.topicBatchers[topicName]
	if !ok {
		log.Debugf("creating new topic batcher")
		if !s.autoCreateTopics {
			return topicBatcher{}, fmt.Errorf("%w: '%s'", seb.ErrTopicNotFound, topicName)
		}

		tb, err = s.makeTopicBatcher(topicName)
		if err != nil {
			return topicBatcher{}, err
		}
		s.topicBatchers[topicName] = tb
	}

	return tb, nil
}

// WithAutoCreateTopic sets whether to automatically create topics if they don't
// already exist.
func WithAutoCreateTopic(autoCreate bool) func(*Opts) {
	return func(o *Opts) {
		o.AutoCreateTopic = autoCreate
	}
}

// WithBatcherFactory sets the WithBatcherFactory to use. This is used to
// configure how long (in time, number of bytes or records) records are kept
// waiting before being persisted to topic storage.
func WithBatcherFactory(f batcherFactory) func(*Opts) {
	return func(o *Opts) {
		o.BatcherFactory = f
	}
}

// WithNullBatcher sets the BatcherFactory to WithNullBatcher. WithNullBatcher
// does not batch records, but persists them one-by-one to topic storage.
func WithNullBatcher() func(*Opts) {
	return func(o *Opts) {
		o.BatcherFactory = NewNullBatcherFactory()
	}
}

func WithOpts(opts Opts) func(*Opts) {
	return func(o *Opts) {
		o.AutoCreateTopic = opts.AutoCreateTopic
		o.BatcherFactory = opts.BatcherFactory
	}
}
