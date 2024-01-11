package recordbatch_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

var log = logger.NewDefault(context.Background())

// TestBlockingBatcherAddReturnValue verifies that the error returned by
// persistRecordBatch() is returned all the way back up to callers of
// batcher.Add().
func TestBlockingBatcherAddReturnValue(t *testing.T) {
	var (
		ctx         context.Context
		cancel      func()
		returnedErr error
	)

	makeContext := func() context.Context {
		return ctx
	}

	persistRecordBatch := func(recordBatch [][]byte) error {
		return returnedErr
	}

	tests := map[string]struct {
		ctx      context.Context
		expected error
	}{
		"err1":     {expected: fmt.Errorf("I'm the error!")},
		"err2":     {expected: fmt.Errorf("other error")},
		"no error": {expected: nil},
	}

	batcher := recordbatch.NewBlockingBatcher(log, makeContext, persistRecordBatch)

	for name, test := range tests {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		t.Run(name, func(t *testing.T) {
			returnedErr = test.expected

			// Test
			got := batcher.Add([]byte{})

			// Verify
			require.ErrorIs(t, got, test.expected)
		})
	}
}

// TestBlockingBatcherAddBlocks verifies that calls to Add() block until
// persistRecordBatch has returned. This ensures that data has been persisted
// before giving control back to the caller.
func TestBlockingBatcherAddBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	makeContext := func() context.Context {
		return ctx
	}

	blockPersistRecordBatch := make(chan struct{})
	returnedErr := fmt.Errorf("all is on fire!")
	persistRecordBatch := func(recordBatch [][]byte) error {
		<-blockPersistRecordBatch
		return returnedErr
	}

	batcher := recordbatch.NewBlockingBatcher(log, makeContext, persistRecordBatch)

	const numRecordBatches = 25

	wg := sync.WaitGroup{}
	wg.Add(numRecordBatches)

	addReturned := atomic.Bool{}
	for _, recordBatch := range tester.MakeRandomRecordBatch(numRecordBatches) {
		recordBatch := recordBatch

		go func() {
			defer wg.Done()

			// Test
			got := batcher.Add(recordBatch)
			addReturned.Store(true)

			// Verify
			require.ErrorIs(t, got, returnedErr)
		}()
	}

	// wait for all above go-routines to be scheduled and block on Add()
	time.Sleep(10 * time.Millisecond)

	// expire ctx to make Batcher persist data (call persistRecordBatch())
	cancel()

	// wait a long time before verifying that none of the Add() callers have returned
	time.Sleep(500 * time.Millisecond)
	require.False(t, addReturned.Load())

	// allow persistRecordBatch to return
	close(blockPersistRecordBatch)

	// wait for persistRecordBatch() return value to propagate to Add() callers
	time.Sleep(10 * time.Millisecond)

	require.True(t, addReturned.Load())

	// ensure that all Add()ers return
	wg.Wait()
}
