package recordbatch_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/micvbang/simple-commit-log/internal/recordbatch"
	"github.com/micvbang/simple-commit-log/internal/tester"
	"github.com/stretchr/testify/require"
)

// TestBatcherAddBlocks verifies that calls to Batcher.Add() block until
// persistRecordBatch has returned, ensuring that data has been persisted before
// giving control back to the caller.
func TestBatcherAddBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	makeContext := func() context.Context {
		return ctx
	}

	expectedRecordBatch := tester.MakeRandomRecordBatch(25)

	persistRecordBatchCalled := false
	persistRecordBatchContinue := make(chan struct{})

	persistRecordBatch := func(recordBatch [][]byte) error {
		<-persistRecordBatchContinue
		persistRecordBatchCalled = true
		return nil
	}

	batcher := recordbatch.NewBatcher(makeContext, persistRecordBatch)

	wg := sync.WaitGroup{}
	wg.Add(len(expectedRecordBatch))

	persistRecordBatchLatency := 500 * time.Millisecond

	t0 := time.Now()
	for _, recordBatch := range expectedRecordBatch {
		recordBatch := recordBatch

		go func() {
			defer wg.Done()

			// Test
			// should block until persistRecordBatch has returned
			batcher.Add(recordBatch)

			// Verify
			require.True(t, time.Since(t0) >= persistRecordBatchLatency)
		}()
	}

	time.Sleep(50 * time.Millisecond) // wait for all above go-routines to block on batcher.Add()

	cancel() // expire ctx to make persistRecordBatch() be called

	// block persistRecordBatch() for a while before returning
	time.Sleep(persistRecordBatchLatency)
	close(persistRecordBatchContinue)

	// Verify
	wg.Wait() // wait until all calls to batcher.Add() have returned

	require.True(t, persistRecordBatchCalled)
}
