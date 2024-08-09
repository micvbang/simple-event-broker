package sebbroker_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/go-helpy/slicey"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebbroker"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/micvbang/simple-event-broker/seberr"
	"github.com/stretchr/testify/require"
)

// TestBlockingBatcherAddReturnValue verifies that the error returned by
// persistRecordBatch() is returned all the way back up to callers of
// batcher.AddRecords().
func TestBlockingBatcherAddReturnValue(t *testing.T) {
	var (
		ctx         context.Context
		cancel      func()
		returnedErr error
	)

	contextFactory := func() context.Context {
		return ctx
	}

	persistRecordBatch := func(batch sebrecords.Batch) ([]uint64, error) {
		if returnedErr != nil {
			return nil, returnedErr
		}

		return make([]uint64, batch.Len()), returnedErr
	}

	tests := map[string]struct {
		ctx      context.Context
		expected error
	}{
		"err1":     {expected: fmt.Errorf("I'm the error!")},
		"err2":     {expected: fmt.Errorf("other error")},
		"no error": {expected: nil},
	}

	batcher := sebbroker.NewBlockingBatcherWithConfig(log, 1024, persistRecordBatch, contextFactory)

	for name, test := range tests {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		t.Run(name, func(t *testing.T) {
			returnedErr = test.expected

			// Test
			_, got := batcher.AddRecords(tester.MakeRandomRecordBatch(0))

			// Verify
			require.ErrorIs(t, got, test.expected)
		})
	}
}

// TestBlockingBatcherAddBlocks verifies that calls to AddRecords() block until
// persistRecordBatch has returned. This ensures that data has been persisted
// before giving control back to the caller.
func TestBlockingBatcherAddBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	contextFactory := func() context.Context {
		return ctx
	}

	blockPersistRecordBatch := make(chan struct{})
	returnedErr := fmt.Errorf("all is on fire!")
	persistRecordBatch := func(batch sebrecords.Batch) ([]uint64, error) {
		<-blockPersistRecordBatch
		return nil, returnedErr
	}

	batcher := sebbroker.NewBlockingBatcherWithConfig(log, 1024, persistRecordBatch, contextFactory)

	const numRecordBatches = 5

	wg := sync.WaitGroup{}
	wg.Add(numRecordBatches)

	addReturned := atomic.Bool{}

	for range numRecordBatches {
		batch := tester.MakeRandomRecordBatch(10)

		go func() {
			defer wg.Done()

			// Test
			_, got := batcher.AddRecords(batch)
			addReturned.Store(true)

			// Verify
			require.ErrorIs(t, got, returnedErr)
		}()
	}

	// wait for all above goroutines to be scheduled and block on AddRecords()
	time.Sleep(5 * time.Millisecond)

	// expire ctx to make Batcher persist data (call persistRecordBatch())
	cancel()

	// wait a long time before verifying that none of the AddRecords() callers have returned
	time.Sleep(10 * time.Millisecond)
	require.False(t, addReturned.Load())

	// allow persistRecordBatch to return
	close(blockPersistRecordBatch)

	// wait for persistRecordBatch() return value to propagate to AddRecords() callers
	time.Sleep(10 * time.Millisecond)

	require.True(t, addReturned.Load())

	// ensure that all AddRecords()ers return
	wg.Wait()
}

// TestBlockingBatcherSoftMax verifies that calls to AddRecords() will block
// until the configured soft max bytes limit is hit, after which it unblocks and
// persists all waiting records.
func TestBlockingBatcherSoftMax(t *testing.T) {
	ctx := context.Background()

	contextFactory := func() context.Context {
		return ctx
	}

	persistRecordBatch := func(batch sebrecords.Batch) ([]uint64, error) {
		return make([]uint64, batch.Len()), nil
	}

	const bytesSoftMax = 10

	batcher := sebbroker.NewBlockingBatcherWithConfig(log, bytesSoftMax, persistRecordBatch, contextFactory)
	addReturned := atomic.Bool{}

	wg := &sync.WaitGroup{}
	wg.Add(bytesSoftMax - 1)

	// add too few bytes to trigger soft max
	for range bytesSoftMax - 1 {
		go func() {
			defer wg.Done()

			_, err := batcher.AddRecords(tester.MakeRandomRecordBatchSize(1, 1))
			require.NoError(t, err)

			addReturned.Store(true)
		}()
	}

	// wait for all above go-routines to be scheduled and block on AddRecords()
	// and ensure that none of the AddRecords() callers have returned
	time.Sleep(5 * time.Millisecond)

	require.False(t, addReturned.Load())

	// add a record hitting the soft max, expecting it to be persisted
	_, err := batcher.AddRecords(tester.MakeRandomRecordBatchSize(1, 1))
	require.NoError(t, err)

	// wait for persistRecordBatch() return value to propagate to AddRecords() callers
	time.Sleep(1 * time.Millisecond)

	require.True(t, addReturned.Load())

	// ensure that all AddRecords()ers return
	wg.Wait()
}

// TestBlockingBatcherSoftMaxSingleRecord verifies that seberr.ErrPayloadTooLarge
// is returned when attempting to add a batch of records that is larger than
// soft max bytes. Additionally, it verifies that a _single_ record with size
// larger than the payload is allowed.
func TestBlockingBatcherSoftMaxSingleRecord(t *testing.T) {
	persistRecordBatch := func(batch sebrecords.Batch) ([]uint64, error) {
		return make([]uint64, batch.Len()), nil
	}

	const bytesSoftMax = 32
	batcher := sebbroker.NewBlockingBatcher(log, time.Second, bytesSoftMax, persistRecordBatch)

	tests := map[string]struct {
		recordSize  int
		records     int
		expectedErr error
	}{
		"single large record": {records: 1, recordSize: bytesSoftMax + 100},
		"many small records":  {records: bytesSoftMax + 1, recordSize: 1, expectedErr: seberr.ErrPayloadTooLarge},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			batch := tester.MakeRandomRecordBatchSize(test.records, test.recordSize)
			_, err := batcher.AddRecords(batch)
			require.ErrorIs(t, err, test.expectedErr)
		})
	}
}

// TestBlockingBatcherConcurrency verifies that concurrent calls to AddRecords()
// and AddRecords() block and returns the correct offsets to all callers.
func TestBlockingBatcherConcurrency(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, s sebtopic.Storage, c *sebcache.Cache) {
		topic, err := sebtopic.New(log, s, "topicName", c, sebtopic.WithCompress(nil))
		require.NoError(t, err)

		batcher := sebbroker.NewBlockingBatcher(log, 5*time.Millisecond, 32*sizey.KB, topic.AddRecords)
		testBlockingBatcherConcurrency(t, batcher, topic)
	})
}

func testBlockingBatcherConcurrency(t *testing.T, batcher sebbroker.RecordBatcher, topic *sebtopic.Topic) {
	ctx := context.Background()

	batches := make([]sebrecords.Batch, 50)
	for i := 0; i < len(batches); i++ {
		batches[i] = tester.MakeRandomRecordBatchSize(inty.RandomN(32)+1, 64*sizey.B)
	}

	const adders = 25

	var recordsAdded atomic.Int32
	stop := make(chan struct{})

	wg := sync.WaitGroup{}
	wg.Add(adders)

	// concurrently add records using AddRecords()
	for range adders {
		go func() {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
				}

				expectedBatch := slicey.Random(batches)

				// Act
				offsets, err := batcher.AddRecords(expectedBatch)
				require.NoError(t, err)

				// Assert
				require.Equal(t, expectedBatch.Len(), len(offsets))

				batch := tester.NewBatch(32, 4096)

				err = topic.ReadRecords(ctx, &batch, offsets[0], len(offsets), 0)
				require.NoError(t, err)

				require.Equal(t, expectedBatch.Len(), batch.Len())
				require.Equal(t, expectedBatch.Data, batch.Data)

				recordsAdded.Add(int32(expectedBatch.Len()))
			}
		}()
	}

	// Run workers concurrently for a while
	for recordsAdded.Load() < 10_000 {
		time.Sleep(50 * time.Millisecond)
	}

	close(stop)
	wg.Wait()
}
