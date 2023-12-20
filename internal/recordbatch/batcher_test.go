package recordbatch_test

import (
	"context"
	"testing"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/stringy"
	"github.com/micvbang/simple-commit-log/internal/recordbatch"
	"github.com/stretchr/testify/require"
)

// TestBatcherMultipleBatchesSimple verifies that Batcher creates batches
// containing only the records added before cancel was called.
func TestBatcherMultipleBatchesSimple(t *testing.T) {
	var (
		ctx    context.Context
		cancel func()
	)

	batcher := recordbatch.NewBatcher(func() context.Context {
		return ctx
	})

	for i := 0; i < 10; i++ {
		ctx, cancel = context.WithCancel(context.Background())

		expectedRecordBatch := makeRandomRecordBatch(1 + inty.RandomN(10))
		for _, record := range expectedRecordBatch {
			batcher.Add(record)
		}

		// the batch must not be finalized before ctx is done.
		requireNoReceive(t, batcher.Output())

		cancel()

		// NOTE: there's a race condition here; if we were to add another
		// batch.Add() at this location, the output would depend on who wins the
		// race of the case selected in an internal `select` statement.

		got := <-batcher.Output()
		require.Equal(t, expectedRecordBatch, got)
	}
}

// TestBatcherOutputNoBatches verifies that there's nothing on the batch.Output
// channel when Add() hasn't been called.
func TestBatcherOutputNoBatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := recordbatch.NewBatcher(func() context.Context {
		return ctx
	})

	requireNoReceive(t, batcher.Output())
}

func makeRandomRecordBatch(size int) [][]byte {
	expectedRecordBatch := make([][]byte, size)
	for i := 0; i < len(expectedRecordBatch); i++ {
		expectedRecordBatch[i] = []byte(stringy.RandomN(10))
	}
	return expectedRecordBatch
}

func requireNoReceive[T any](t *testing.T, ch chan T) {
	select {
	case <-ch:
		require.FailNow(t, "no receive expected")
	default:
	}
}
