package sebtopic_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/micvbang/simple-event-broker/seberr"
	"github.com/stretchr/testify/require"
)

func TestTopicReadLimitsDoNotWrapAtFourGiB(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("requires a 64-bit byte limit")
	}

	cache, err := sebcache.NewMemoryCache(log)
	require.NoError(t, err)

	topic, err := sebtopic.New(log, sebtopic.NewMemoryStorage(log), "topic", cache)
	require.NoError(t, err)

	_, err = topic.AddRecords(sebrecords.NewBatch([]uint32{1, 1}, []byte("ab")))
	require.NoError(t, err)

	limit := uint64(1) << 32

	for _, maxBytes := range []uint64{limit, limit + 1} {
		got := tester.NewBatch(2, 16)
		require.NoError(t, topic.ReadRecords(context.Background(), &got, 0, 2, int(maxBytes)))
		require.Equal(t, []byte("ab"), got.Data)
	}

	for _, limits := range [][2]int{{-1, 1}, {1, -1}} {
		got := tester.NewBatch(2, 16)
		require.ErrorIs(t, topic.ReadRecords(context.Background(), &got, 0, limits[0], limits[1]), seberr.ErrBadInput)
		require.Empty(t, got.Data)
	}
}
