package recordbatch

import (
	"context"
	"time"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

type Persist func(RecordBatch) ([]uint64, error)

type blockedAdd struct {
	record   Record
	response chan<- addResponse
}

type addResponse struct {
	offset uint64
	err    error
}

type BlockingBatcher struct {
	log          logger.Logger
	bytesSoftMax int

	contextFactory func() context.Context
	callers        chan blockedAdd

	persist Persist
}

func NewBlockingBatcher(log logger.Logger, blockTime time.Duration, bytesSoftMax int, persistRecordBatch Persist) *BlockingBatcher {
	return NewBlockingBatcherWithConfig(log, bytesSoftMax, persistRecordBatch, NewContextFactory(blockTime))
}

func NewBlockingBatcherWithConfig(log logger.Logger, bytesSoftMax int, persist Persist, contextFactory func() context.Context) *BlockingBatcher {
	b := &BlockingBatcher{
		log:            log,
		callers:        make(chan blockedAdd, 32),
		contextFactory: contextFactory,
		persist:        persist,
		bytesSoftMax:   bytesSoftMax,
	}

	// NOTE: this goroutine is never stopped
	go b.collectBatches()

	return b
}

// AddRecord adds record to the ongoing record batch and blocks until
// persistRecordBatch() has been called and completed.
//
// persistRecordBatch() will be called once the most recent context returned by
// contextFactory() has expired, or bytesSoftMax has been reached. Beware of
// long-lived contexts returned by contextFactory(); AddRecord() could block all
// callers until the context expires.
func (b *BlockingBatcher) AddRecord(r Record) (uint64, error) {
	responses := make(chan addResponse)

	b.callers <- blockedAdd{
		response: responses,
		record:   r,
	}

	// block caller until record has been peristed (or persisting failed)
	response := <-responses
	return response.offset, response.err
}

func (b *BlockingBatcher) collectBatches() {
	for {
		blockedCallers := make([]blockedAdd, 0, 64)

		// block until AddRecord() is called, starting a new batch collection
		blockedCaller := <-b.callers
		blockedCallers = append(blockedCallers, blockedCaller)
		batchBytes := len(blockedCaller.record)

		ctx, cancel := context.WithCancel(b.contextFactory())
		defer cancel()
		t0 := time.Now()

	innerLoop:
		for {
			select {

			case blockedCaller := <-b.callers:
				blockedCallers = append(blockedCallers, blockedCaller)
				batchBytes += len(blockedCaller.record)
				b.log.Debugf("added record to batch (%d)", len(blockedCallers))
				if batchBytes >= b.bytesSoftMax {
					b.log.Debugf("batch size exceeded soft max (%d/%d), collecting", batchBytes, b.bytesSoftMax)

					// NOTE: this will not necessarily cause the batch collection
					// branch of this select to be invoked; if there's more adds on
					// handledAdds, it's likely that this branch will continue to
					// process one or more of those.
					cancel()
				}

			case <-ctx.Done():
				b.log.Debugf("batch collection time: %v", time.Since(t0))

				recordBatch := make(RecordBatch, len(blockedCallers))
				for i, add := range blockedCallers {
					recordBatch[i] = add.record
				}

				// block until recordBatch is persisted or persisting failed
				offsets, err := b.persist(recordBatch)
				b.log.Debugf("%d records persisted (err: %v)", len(recordBatch), err)
				if err != nil {
					b.log.Debugf("reporting error to %d waiting callers", len(recordBatch))

					// offsets should be 0 in all responses
					offsets = make([]uint64, len(recordBatch))
				}

				// unblock callers
				for i, blockedCaller := range blockedCallers {
					blockedCaller.response <- addResponse{
						offset: offsets[i],
						err:    err,
					}
					close(blockedCaller.response)
				}

				b.log.Debugf("done reporting results")
				break innerLoop
			}
		}
	}
}

func NewContextFactory(blockTime time.Duration) func() context.Context {
	return func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), blockTime)
		go func() {
			// We have to cancel the context. Just ensure that it's cancelled at
			// some point in the future.
			time.Sleep(blockTime * 2)
			cancel()
		}()

		return ctx
	}
}
