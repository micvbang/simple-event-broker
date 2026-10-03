package sebbroker_test

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/sebbroker"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
)

//go:noinline
func batchingPayloadContext(parent context.Context) context.Context {
	return context.WithValue(parent, batchingContextKey{}, make([]byte, 64*1024))
}

type batchingContextKey struct{}

// Completed flushes must release their context values. Heap profiling isolates
// these allocations from other objects retained by background goroutines.
func TestBlockingBatcherReleasesFlushedContexts(t *testing.T) {
	oldRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	defer func() {
		runtime.MemProfileRate = oldRate
	}()

	parent, cancel := context.WithCancel(context.Background())
	cancel()
	batcher := sebbroker.NewBlockingBatcherWithConfig(log, 1024,
		func(batch sebrecords.Batch) ([]uint64, error) {
			return make([]uint64, batch.Len()), nil
		},
		func() context.Context {
			return batchingPayloadContext(parent)
		})
	const flushes = 128

	for range flushes {
		if _, err := batcher.AddRecords(sebrecords.NewBatch([]uint32{1}, []byte("x"))); err != nil {
			t.Fatal(err)
		}
	}

	runtime.GC()
	runtime.GC()
	n, _ := runtime.MemProfile(nil, false)
	profile := make([]runtime.MemProfileRecord, n+100)
	n, ok := runtime.MemProfile(profile, false)
	if !ok {
		t.Fatal("heap profile buffer insufficient")
	}

	var retained int64

	for _, record := range profile[:n] {
		if record.InUseObjects() == 0 || record.InUseBytes()/record.InUseObjects() != 64*1024 {
			continue
		}

		frames := runtime.CallersFrames(record.Stack())

		for {
			frame, more := frames.Next()
			if strings.HasSuffix(frame.Function, ".batchingPayloadContext") {
				retained += record.InUseObjects()
				break
			}

			if !more {
				break
			}
		}
	}

	// Allow the most recent context to remain on the collector stack while it
	// waits for the next caller. Retention must not grow with completed flushes.
	if retained > 1 {
		t.Fatalf("retained %d of %d completed-flush contexts", retained, flushes)
	}
}
