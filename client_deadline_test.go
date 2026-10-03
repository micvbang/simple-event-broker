package seb_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebbroker"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/stretchr/testify/require"
)

// A backing storage read blocked until its request deadline models a slow
// network fetch. Everything else delegates to the ordinary storage.
type deadlineStorageForClient struct {
	*sebtopic.MemoryTopicStorage
}

func (s *deadlineStorageForClient) Reader(ctx context.Context, key string) (io.ReadCloser, error) {
	if strings.HasSuffix(key, "000000000001.record_batch") {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	return s.MemoryTopicStorage.Reader(ctx, key)
}

func TestRecordClientGetRecordsPreservesRecordsBeforeDeadline(t *testing.T) {
	log := logger.NewDefault(context.Background())
	cache, err := sebcache.NewMemoryCache(log)
	require.NoError(t, err)

	storage := &deadlineStorageForClient{sebtopic.NewMemoryStorage(log)}
	broker := sebbroker.New(log, func(log logger.Logger, name string) (*sebtopic.Topic, error) {
		return sebtopic.New(log, storage, name, cache, sebtopic.WithCompress(nil))
	}, sebbroker.WithNullBatcher())

	for _, record := range []string{"first", "second"} {
		_, err = broker.AddRecords("proof", sebrecords.NewBatch([]uint32{uint32(len(record))}, []byte(record)))
		require.NoError(t, err)
	}

	require.NoError(t, cache.EvictLeastRecentlyUsed(0))

	// Establish that the actual broker and Topic retain the first record when
	// the second backing-storage fetch runs out of time.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	batch := tester.NewBatch(2, 64)
	err = broker.GetRecords(ctx, &batch, "proof", 0, 2, 64)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, [][]byte{[]byte("first")}, batch.IndividualRecords())

	srv := tester.HTTPServer(t, tester.HTTPDependencies(broker))
	defer srv.Close()

	client, err := seb.NewRecordClient(srv.Server.URL, tester.DefaultAPIKey)
	require.NoError(t, err)

	records, err := client.GetRecords("proof", 0, seb.GetRecordsInput{
		MaxRecords: 2,
		Buffer:     make([]byte, 64),
		Timeout:    10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("first")}, records, "HTTP must preserve records collected before the deadline")
}
