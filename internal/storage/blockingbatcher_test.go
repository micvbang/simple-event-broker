package storage_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

// TestBlockingBatcherAddReturnValue verifies that the error returned by
// persistRecordBatch() is returned all the way back up to callers of
// batcher.AddRecord().
func TestBlockingBatcherAddReturnValue(t *testing.T) {
	var (
		ctx         context.Context
		cancel      func()
		returnedErr error
	)

	contextFactory := func() context.Context {
		return ctx
	}

	persistRecordBatch := func(rb recordbatch.RecordBatch) ([]uint64, error) {
		if returnedErr != nil {
			return nil, returnedErr
		}

		return make([]uint64, len(rb)), returnedErr
	}

	tests := map[string]struct {
		ctx      context.Context
		expected error
	}{
		"err1":     {expected: fmt.Errorf("I'm the error!")},
		"err2":     {expected: fmt.Errorf("other error")},
		"no error": {expected: nil},
	}

	batcher := storage.NewBlockingBatcherWithConfig(log, 1024, persistRecordBatch, contextFactory)

	for name, test := range tests {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		t.Run(name, func(t *testing.T) {
			returnedErr = test.expected

			// Test
			_, got := batcher.AddRecord(recordbatch.Record{})

			// Verify
			require.ErrorIs(t, got, test.expected)
		})
	}
}

// TestBlockingBatcherAddBlocks verifies that calls to AddRecord() block until
// persistRecordBatch has returned. This ensures that data has been persisted
// before giving control back to the caller.
func TestBlockingBatcherAddBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	contextFactory := func() context.Context {
		return ctx
	}

	blockPersistRecordBatch := make(chan struct{})
	returnedErr := fmt.Errorf("all is on fire!")
	persistRecordBatch := func(rb recordbatch.RecordBatch) ([]uint64, error) {
		<-blockPersistRecordBatch
		return nil, returnedErr
	}

	batcher := storage.NewBlockingBatcherWithConfig(log, 1024, persistRecordBatch, contextFactory)

	const numRecordBatches = 5

	wg := sync.WaitGroup{}
	wg.Add(numRecordBatches)

	addReturned := atomic.Bool{}
	for _, recordBatch := range tester.MakeRandomRecordBatch(numRecordBatches) {
		recordBatch := recordBatch

		go func() {
			defer wg.Done()

			// Test
			_, got := batcher.AddRecord(recordBatch)
			addReturned.Store(true)

			// Verify
			require.ErrorIs(t, got, returnedErr)
		}()
	}

	// wait for all above go-routines to be scheduled and block on AddRecord()
	time.Sleep(5 * time.Millisecond)

	// expire ctx to make Batcher persist data (call persistRecordBatch())
	cancel()

	// wait a long time before verifying that none of the AddRecord() callers have returned
	time.Sleep(10 * time.Millisecond)
	require.False(t, addReturned.Load())

	// allow persistRecordBatch to return
	close(blockPersistRecordBatch)

	// wait for persistRecordBatch() return value to propagate to AddRecord() callers
	time.Sleep(10 * time.Millisecond)

	require.True(t, addReturned.Load())

	// ensure that all AddRecord()ers return
	wg.Wait()
}

// TestBlockingBatcherSoftMax verifies that calls to AddRecord() will block
// until the configured soft max bytes limit is hit, after which it unblocks and
// persists all waiting records.
func TestBlockingBatcherSoftMax(t *testing.T) {
	ctx := context.Background()

	contextFactory := func() context.Context {
		return ctx
	}

	persistRecordBatch := func(rb recordbatch.RecordBatch) ([]uint64, error) {
		return make([]uint64, len(rb)), nil
	}

	const bytesSoftMax = 10

	batcher := storage.NewBlockingBatcherWithConfig(log, bytesSoftMax, persistRecordBatch, contextFactory)
	addReturned := atomic.Bool{}

	wg := &sync.WaitGroup{}
	wg.Add(bytesSoftMax - 1)

	// add too few bytes to trigger soft max
	for range bytesSoftMax - 1 {
		go func() {
			defer wg.Done()

			_, err := batcher.AddRecord([]byte("1"))
			require.NoError(t, err)

			addReturned.Store(true)
		}()
	}

	// wait for all above go-routines to be scheduled and block on AddRecord()
	// and ensure that none of the AddRecord() callers have returned
	time.Sleep(5 * time.Millisecond)

	require.False(t, addReturned.Load())

	// add a record hitting the soft max, expecting it to be persisted
	_, err := batcher.AddRecord([]byte("1"))
	require.NoError(t, err)

	// wait for persistRecordBatch() return value to propagate to AddRecord() callers
	time.Sleep(1 * time.Millisecond)

	require.True(t, addReturned.Load())

	// ensure that all AddRecord()ers return
	wg.Wait()
}
