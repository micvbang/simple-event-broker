package sebbroker_test

import (
	"context"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/sebbroker"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/micvbang/simple-event-broker/seberr"
	"github.com/stretchr/testify/require"
)

func TestBrokerRejectsUnsafeTopicNames(t *testing.T) {
	storage := sebtopic.NewMemoryStorage(log)
	cache, err := sebcache.NewMemoryCache(log)
	require.NoError(t, err)

	broker := sebbroker.New(log, sebbroker.NewTopicFactory(storage, cache), sebbroker.WithNullBatcher())
	input := sebrecords.NewBatch([]uint32{1}, []byte("A"))
	_, err = broker.AddRecords("orders", input)
	require.NoError(t, err)

	for _, name := range []string{
		"",
		".",
		"..",
		"/orders",
		"orders/",
		"orders/.",
		"other/../orders",
		"orders/child",
		"orders\\child",
		"orders\x00",
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, broker.CreateTopic(name), seberr.ErrBadInput)

			_, err := broker.AddRecords(name, sebrecords.NewBatch([]uint32{1}, []byte("B")))
			require.ErrorIs(t, err, seberr.ErrBadInput)

			_, err = broker.Metadata(name)
			require.ErrorIs(t, err, seberr.ErrBadInput)

			out := sebrecords.NewBatch(make([]uint32, 0, 1), make([]byte, 0, 16))
			_, err = broker.GetRecord(&out, name, 0)
			require.ErrorIs(t, err, seberr.ErrBadInput)
			require.ErrorIs(t, broker.GetRecords(context.Background(), &out, name, 0, 1, 0), seberr.ErrBadInput)
		})
	}

	for _, name := range []string{"order-v2", "order_v2", "order.v2", ".orders", "注文"} {
		require.NoError(t, broker.CreateTopic(name))
	}

	// Invalid names must not overwrite a committed record, including on restart.
	freshCache, err := sebcache.NewMemoryCache(log)
	require.NoError(t, err)

	reopened := sebbroker.New(log, sebbroker.NewTopicFactory(storage, freshCache), sebbroker.WithNullBatcher())
	out := sebrecords.NewBatch(make([]uint32, 0, 1), make([]byte, 0, 16))
	got, err := reopened.GetRecord(&out, "orders", 0)
	require.NoError(t, err)
	require.Equal(t, []byte("A"), got)
}
