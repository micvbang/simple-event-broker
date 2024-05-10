package storage

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

func NewS3TopicFactory(cfg aws.Config, s3BucketName string, cache *cache.Cache, compress topic.Compress) TopicFactory {
	return func(log logger.Logger, topicName string) (*topic.Topic, error) {
		storageLogger := log.Name("s3 storage").WithField("topic-name", topicName).WithField("bucket", s3BucketName)

		s3Client := s3.NewFromConfig(cfg)
		s3Storage := topic.NewS3Storage(storageLogger, s3Client, s3BucketName, "")
		return topic.New(log, s3Storage, topicName, cache, compress)
	}
}

func NewTopicFactory(ts topic.Storage, cache *cache.Cache) TopicFactory {
	return func(log logger.Logger, topicName string) (*topic.Topic, error) {
		return topic.New(log, ts, topicName, cache, &topic.Gzip{})
	}
}

type BatcherFactory func(logger.Logger, *topic.Topic) RecordBatcher

func NewBlockingBatcherFactory(blockTime time.Duration, batchBytesMax int) BatcherFactory {
	return func(log logger.Logger, ts *topic.Topic) RecordBatcher {
		batchLogger := log.Name("blocking batcher")

		return NewBlockingBatcher(batchLogger, blockTime, batchBytesMax, func(b recordbatch.RecordBatch) ([]uint64, error) {
			t0 := time.Now()
			offsets, err := ts.AddRecordBatch(b)
			batchLogger.Infof("persisting to s3: %v", time.Since(t0))
			return offsets, err
		})
	}
}

func NewNullBatcherFactory() BatcherFactory {
	return func(l logger.Logger, t *topic.Topic) RecordBatcher {
		return NewNullBatcher(t.AddRecordBatch)
	}
}
