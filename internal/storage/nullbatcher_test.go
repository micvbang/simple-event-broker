package storage_test

import (
	"testing"

	"github.com/micvbang/simple-event-broker/internal/cache"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/micvbang/simple-event-broker/internal/topic"
	"github.com/stretchr/testify/require"
)

// TestNullBatcherConcurrency verifies that concurrent calls to AddRecords() and
// AddRecord() block and returns the correct offsets to all callers.
func TestNullBatcherConcurrency(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, s topic.Storage, c *cache.Cache) {
		topic, err := topic.New(log, s, "topicName", c, nil)
		require.NoError(t, err)

		batcher := storage.NewNullBatcher(topic.AddRecordBatch)
		testBlockingBatcherConcurrency(t, batcher, topic)
	})
}
