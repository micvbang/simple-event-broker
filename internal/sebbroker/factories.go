package sebbroker

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
)

type TopicFactory func(_ logger.Logger, topicName string) (*sebtopic.Topic, error)

func NewS3TopicFactory(cfg aws.Config, s3BucketName string, cache *sebcache.Cache) TopicFactory {
	return func(log logger.Logger, topicName string) (*sebtopic.Topic, error) {
		storageLogger := log.Name("s3 storage").WithField("topic-name", topicName).WithField("bucket", s3BucketName)

		s3Client := s3.NewFromConfig(cfg)
		s3Storage := sebtopic.NewS3Storage(storageLogger, s3Client, s3BucketName, "")
		return sebtopic.New(log, s3Storage, topicName, cache)
	}
}

func NewTopicFactory(ts sebtopic.Storage, cache *sebcache.Cache) TopicFactory {
	return func(log logger.Logger, topicName string) (*sebtopic.Topic, error) {
		return sebtopic.New(log, ts, topicName, cache)
	}
}

type batcherFactory func(logger.Logger, *sebtopic.Topic) RecordBatcher

func NewBlockingBatcherFactory(blockTime time.Duration, batchBytesMax int) batcherFactory {
	return func(log logger.Logger, t *sebtopic.Topic) RecordBatcher {
		log = log.Name("blocking batcher")

		persist := func(batch sebrecords.Batch) ([]uint64, error) {
			t0 := time.Now()
			offsets, err := t.AddRecords(batch)
			log.Infof("persisting to storage: %v", time.Since(t0))
			return offsets, err
		}

		return NewBlockingBatcher(log, blockTime, batchBytesMax, persist)
	}
}

func NewNullBatcherFactory() batcherFactory {
	return func(l logger.Logger, t *sebtopic.Topic) RecordBatcher {
		return NewNullBatcher(t.AddRecords)
	}
}
