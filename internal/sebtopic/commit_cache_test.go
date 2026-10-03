package sebtopic_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/micvbang/simple-event-broker/seberr"
	"github.com/stretchr/testify/require"
)

type unavailableWriteCache struct {
	sebcache.Storage
	fail   bool
	writes int
}

func (s *unavailableWriteCache) Writer(key string) (io.WriteCloser, error) {
	s.writes++
	if s.fail {
		return nil, errors.New("cache unavailable")
	}

	return s.Storage.Writer(key)
}

func TestTopicCommitDoesNotDependOnCache(t *testing.T) {
	cs := &unavailableWriteCache{
		Storage: sebcache.NewMemoryStorage(log),
		fail:    true,
	}
	cache, err := sebcache.New(log, cs)
	require.NoError(t, err)

	topic, err := sebtopic.New(log, sebtopic.NewMemoryStorage(log), "topic", cache)
	require.NoError(t, err)

	offsets, err := topic.AddRecords(sebrecords.NewBatch([]uint32{3, 3}, []byte("abcdef")))
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1}, offsets)
	require.Zero(t, cs.writes, "committing must never publish a partial cache entry")

	_, err = cache.Reader(sebtopic.RecordBatchKey("topic", 0))
	require.ErrorIs(t, err, seberr.ErrNotInCache)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, topic.OffsetCond.Wait(ctx, 1), "committed offsets must be broadcast without cache work")

	cs.fail = false
	got := tester.NewBatch(2, 64)
	require.NoError(t, topic.ReadRecords(context.Background(), &got, 0, 2, 0))
	require.Equal(t, []byte("abcdef"), got.Data)
	require.Equal(t, []uint32{3, 3}, got.Sizes)
}
