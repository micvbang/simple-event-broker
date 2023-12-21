package recordbatch

import (
	"context"
	"sync"
)

type Batcher struct {
	mu              sync.Mutex
	collectingBatch bool
	addResults      []chan<- error

	makeContext func() context.Context
	records     chan []byte

	persistRecordBatch func([][]byte) error
}

func NewBatcher(makeContext func() context.Context, persistRecordBatch func([][]byte) error) *Batcher {
	return &Batcher{
		mu:                 sync.Mutex{},
		records:            make(chan []byte),
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
func (b *Batcher) Add(record []byte) error {
	result := make(chan error)

	b.mu.Lock()
	{
		if !b.collectingBatch {
			b.collectingBatch = true
			b.addResults = make([]chan<- error, 0, 16)
			go b.collectBatch(b.makeContext())
		}
		b.addResults = append(b.addResults, result)
	}
	b.mu.Unlock()

	b.records <- record

	// block until record has been peristed
	return <-result
}

func (b *Batcher) collectBatch(ctx context.Context) {
	recordBatch := make([][]byte, 0, 16)
	for {
		select {

		case record := <-b.records:
			recordBatch = append(recordBatch, record)

		case <-ctx.Done():
			err := b.persistRecordBatch(recordBatch)

			// report result to waiting Add()ers
			for _, result := range b.addResults {
				result <- err
			}

			b.mu.Lock()
			{
				b.collectingBatch = false
			}
			b.mu.Unlock()

			return
		}
	}
}
