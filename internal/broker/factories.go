package broker

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/micvbang/simple-event-broker/internal/cache"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/topic"
)

type TopicFactory func(_ logger.Logger, topicName string) (*topic.Topic, error)

func NewS3TopicFactory(cfg aws.Config, s3BucketName string, cache *cache.Cache) TopicFactory {
	return func(log logger.Logger, topicName string) (*topic.Topic, error) {
		storageLogger := log.Name("s3 storage").WithField("topic-name", topicName).WithField("bucket", s3BucketName)

		s3Client := s3.NewFromConfig(cfg)
		s3Storage := topic.NewS3Storage(storageLogger, s3Client, s3BucketName, "")
		return topic.New(log, s3Storage, topicName, cache)
	}
}

func NewTopicFactory(ts topic.Storage, cache *cache.Cache) TopicFactory {
	return func(log logger.Logger, topicName string) (*topic.Topic, error) {
		return topic.New(log, ts, topicName, cache)
	}
}

type batcherFactory func(logger.Logger, *topic.Topic) RecordBatcher

func NewBlockingBatcherFactory(blockTime time.Duration, batchBytesMax int) batcherFactory {
	return func(log logger.Logger, t *topic.Topic) RecordBatcher {
		log = log.Name("blocking batcher")

		persist := func(b []recordbatch.Record) ([]uint64, error) {
			t0 := time.Now()
			offsets, err := t.AddRecords(b)
			log.Infof("persisting to storage: %v", time.Since(t0))
			return offsets, err
		}

		return NewBlockingBatcher(log, blockTime, batchBytesMax, persist)
	}
}

func NewNullBatcherFactory() batcherFactory {
	return func(l logger.Logger, t *topic.Topic) RecordBatcher {
		return NewNullBatcher(t.AddRecords)
	}
}
