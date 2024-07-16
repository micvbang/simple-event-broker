package sebbroker_test

import (
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebbroker"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/stretchr/testify/require"
)

// TestNullBatcherConcurrency verifies that concurrent calls to AddRecords() and
// AddRecords() block and returns the correct offsets to all callers.
func TestNullBatcherConcurrency(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, s sebtopic.Storage, c *sebcache.Cache) {
		topic, err := sebtopic.New(log, s, "topicName", c, sebtopic.WithCompress(nil))
		require.NoError(t, err)

		batcher := sebbroker.NewNullBatcher(topic.AddRecords)
		testBlockingBatcherConcurrency(t, batcher, topic)
	})
}
