package recordbatch

import (
	"context"
	"sync"
	"time"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

type blockedAdd struct {
	record Record
	err    chan<- error
}

type BlockingBatcher struct {
	log             logger.Logger
	mu              sync.Mutex
	collectingBatch bool

	makeContext func() context.Context
	blockedAdds chan blockedAdd

	persistRecordBatch func(RecordBatch) error
}

func NewBlockingBatcher(log logger.Logger, makeContext func() context.Context, persistRecordBatch func(RecordBatch) error) *BlockingBatcher {
	return &BlockingBatcher{
		log:                log,
		mu:                 sync.Mutex{},
		blockedAdds:        make(chan blockedAdd, 32),
		makeContext:        makeContext,
		persistRecordBatch: persistRecordBatch,
	}
}

// Add adds record to the ongoing record batch and blocks until
// persistRecordBatch() has been called and completed.
//
// persistRecordBatch() will be called once the most recent context
// returned by makeContext() has expired. This means that, if makeContext()
// returns a very long living context, Add() will block for a long time.
func (b *BlockingBatcher) Add(r Record) error {
	errCh := make(chan error)

	b.mu.Lock()
	{
		if !b.collectingBatch {
			b.collectingBatch = true
			go b.collectBatch(b.makeContext())
		}
	}
	b.mu.Unlock()

	b.blockedAdds <- blockedAdd{
		err:    errCh,
		record: r,
	}

	// block until record has been peristed
	return <-errCh
}

func (b *BlockingBatcher) collectBatch(ctx context.Context) {
	handledAdds := make([]blockedAdd, 0, 64)

	t0 := time.Now()

	for {
		select {

		case blockedAdd := <-b.blockedAdds:
			handledAdds = append(handledAdds, blockedAdd)
			b.log.Debugf("added record to batch (%d)", len(handledAdds))

		case <-ctx.Done():
			b.log.Debugf("batch collection time: %v", time.Since(t0))

			recordBatch := make(RecordBatch, len(handledAdds))
			for i, add := range handledAdds {
				recordBatch[i] = add.record
			}

			err := b.persistRecordBatch(recordBatch)
			b.log.Debugf("%d records persisted (err: %v)", len(recordBatch), err)
			if err != nil {
				b.log.Debugf("reporting error to %d waiting add()ers", len(recordBatch))
				for _, handledAdd := range handledAdds {
					handledAdd.err <- err
				}
			}

			// Unblock Add()ers
			for _, handledAdd := range handledAdds {
				close(handledAdd.err)
			}

			b.log.Debugf("done reporting results")

			b.mu.Lock()
			{
				b.collectingBatch = false
			}
			b.mu.Unlock()

			return
		}
	}
}
