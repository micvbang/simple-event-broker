package sebbroker_test

import (
	"context"
	"testing"
	"time"

	"github.com/micvbang/simple-event-broker/internal/sebbroker"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/stretchr/testify/require"
)

func TestBlockingBatcherFirstRecordTriggersByteThreshold(t *testing.T) {
	for _, payload := range []string{"ab", "abc"} {
		t.Run(payload, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			persisted := make(chan sebrecords.Batch, 1)
			batcher := sebbroker.NewBlockingBatcherWithConfig(log, 2, func(batch sebrecords.Batch) ([]uint64, error) {
				persisted <- batch

				return []uint64{42}, nil
			}, func() context.Context {
				return ctx
			})
			type result struct {
				offsets []uint64
				err     error
			}
			returned := make(chan result, 1)

			go func() {
				offsets, err := batcher.AddRecords(sebrecords.NewBatch([]uint32{uint32(len(payload))}, []byte(payload)))
				returned <- result{offsets, err}
			}()

			select {
			case batch := <-persisted:
				require.Equal(t, payload, string(batch.Data))
			case <-time.After(time.Second):
				t.Fatal("first record did not trigger persistence without a timer or another caller")
			}

			select {
			case res := <-returned:
				require.NoError(t, res.err)
				require.Equal(t, []uint64{42}, res.offsets)
			case <-time.After(time.Second):
				t.Fatal("adder did not receive its committed offset")
			}
		})
	}
}
