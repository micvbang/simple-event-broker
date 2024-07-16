package sebbroker

import (
	"context"
	"fmt"
	"time"

	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
)

type Persist func(sebrecords.Batch) ([]uint64, error)

type blockedAdd struct {
	batch    sebrecords.Batch
	response chan<- addResponse
}

type addResponse struct {
	offsets []uint64
	err     error
}

// BlockingBatcher is responsible for batching records before persisting them
// into topic storage. Batching is done to amortize the cost of persisting data
// to topic storage. This is helpful when the topic storage is an object store
// that is expected to have large latencies and per-call $ costs.
//
// BlockingBatcher collects records for a batch until either
// 1) the block time has elapsed
// 2) the soft maximum number of bytes has been reached
//
// persistRecordBatch() will be called once the most recent context returned by
// contextFactory() has expired, or bytesSoftMax has been reached. Beware of
// long-lived contexts returned by contextFactory() as this could block all
// adders until the context expires!
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

// AddRecords adds records to the batch that is currently being built and blocks
// until persistRecordBatch() has been called and completed; when AddRecords returns,
// the given record has either been persisted to topic storage or failed.
func (b *BlockingBatcher) AddRecords(batch sebrecords.Batch) ([]uint64, error) {
	// NOTE: allows single records larger than bytesSoftMax; this is done to
	// avoid making it impossible to add records of unexpectedly large size.
	if len(batch.Data()) > b.bytesSoftMax && batch.Len() > 1 {
		return nil, fmt.Errorf("%w (%d bytes), bytes max is %d", seb.ErrPayloadTooLarge, len(batch.Data()), b.bytesSoftMax)
	}

	responses := make(chan addResponse)

	b.callers <- blockedAdd{
		response: responses,
		batch:    batch,
	}

	// block caller until record has been peristed (or persisting failed)
	response := <-responses

	if len(response.offsets) != batch.Len() {
		// This is not supposed to happen; if it does, we can't trust b.persist().
		panic(fmt.Sprintf("unexpected number of offsets returned %d, expected %d", len(response.offsets), batch.Len()))
	}
	return response.offsets, response.err

}

// AddRecord adds record to the batch that is currently being built and blocks
// until persistRecordBatch() has been called and completed; when AddRecord returns,
// the given record has either been persisted to topic storage or failed.
func (b *BlockingBatcher) AddRecord(record sebrecords.Record) (uint64, error) {
	offsets, err := b.AddRecords(sebrecords.NewBatch([]uint32{uint32(len(record))}, record))
	if err != nil {
		return 0, err
	}

	if len(offsets) != 1 {
		// This is not supposed to happen; if it does, we can't trust b.persist().
		panic(fmt.Sprintf("unexpected number of offsets returned %d, expected 1", len(offsets)))
	}

	return offsets[0], nil
}

func (b *BlockingBatcher) collectBatches() {
	for {
		blockedCallers := make([]blockedAdd, 0, 64)

		// block until there are records coming in, starting a new batch collection
		blockedCaller := <-b.callers
		blockedCallers = append(blockedCallers, blockedCaller)

		batchBytes := len(blockedCaller.batch.Data())
		batchRecords := blockedCaller.batch.Len()

		ctx, cancel := context.WithCancel(b.contextFactory())
		defer cancel()
		t0 := time.Now()

	innerLoop:
		for {
			select {

			case blockedCaller := <-b.callers:
				blockedCallers = append(blockedCallers, blockedCaller)
				batchBytes += len(blockedCaller.batch.Data())
				batchRecords += blockedCaller.batch.Len()

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

				recordData := make([]byte, 0, batchBytes)
				recordSizes := make([]uint32, 0, batchRecords)
				for _, add := range blockedCallers {
					recordData = append(recordData, add.batch.Data()...)
					recordSizes = append(recordSizes, add.batch.Sizes()...)
				}

				// block until records are persisted or persisting failed
				offsets, err := b.persist(sebrecords.NewBatch(recordSizes, recordData))
				b.log.Debugf("%d records persisted (err: %v)", len(recordSizes), err)
				if err != nil {
					b.log.Debugf("reporting error to %d waiting callers", len(recordSizes))

					// offsets should be 0 in all error responses
					offsets = make([]uint64, len(recordSizes))
				}

				// unblock callers
				offsetIndex := 0
				for _, blockedCaller := range blockedCallers {
					offsetMax := offsetIndex + blockedCaller.batch.Len()
					blockedCaller.response <- addResponse{
						offsets: offsets[offsetIndex:offsetMax],
						err:     err,
					}
					offsetIndex = offsetMax
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
