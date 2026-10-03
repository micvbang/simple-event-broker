package sebbroker_test

import (
	"context"
	"testing"
	"time"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebbroker"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/stretchr/testify/require"
)

func TestBrokerWaitsForFirstRecord(t *testing.T) {
	cache, err := sebcache.NewMemoryCache(log)
	require.NoError(t, err)

	storage := sebtopic.NewMemoryStorage(log)
	var topic *sebtopic.Topic
	broker := sebbroker.New(log, func(l logger.Logger, name string) (*sebtopic.Topic, error) {
		var err error
		topic, err = sebtopic.New(l, storage, name, cache)

		return topic, err
	}, sebbroker.WithNullBatcher())
	_, err = broker.Metadata("topic")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got := tester.NewBatch(1, 64)
	returned := make(chan error, 1)

	go func() {
		returned <- broker.GetRecords(ctx, &got, "topic", 0, 1, 0)
	}()

	require.Eventually(t, func() bool {
		return topic.OffsetCond.Waiting() == 1
	}, time.Second, time.Millisecond)

	_, err = broker.AddRecords("topic", sebrecords.NewBatch([]uint32{5}, []byte("first")))
	require.NoError(t, err)
	require.NoError(t, <-returned)
	require.Equal(t, []byte("first"), got.Data)
}
