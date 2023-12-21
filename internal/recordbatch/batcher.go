package recordbatch

import (
	"context"
	"fmt"
	"sync"
)

type Batcher struct {
	recordBatch [][]byte

	mu              sync.Mutex
	collectingBatch bool
	blockAdd        chan struct{}

	makeContext func() context.Context
	records     chan []byte

	persistRecordBatch func([][]byte) error
}

func NewBatcher(makeContext func() context.Context, persistRecordBatch func([][]byte) error) *Batcher {
	return &Batcher{
		recordBatch:        make([][]byte, 0, 16),
		mu:                 sync.Mutex{},
		records:            make(chan []byte),
		blockAdd:           nil,
		makeContext:        makeContext,
		persistRecordBatch: persistRecordBatch,
	}
}

func (b *Batcher) Add(record []byte) error {
	b.mu.Lock()
	if !b.collectingBatch {
		b.collectingBatch = true
		b.blockAdd = make(chan struct{})
		go b.collectBatch(b.makeContext())
	}
	b.mu.Unlock()

	b.records <- record

	// block until record has been peristed
	<-b.blockAdd
	return nil
}

func (b *Batcher) collectBatch(ctx context.Context) {
	recordBatch := make([][]byte, 0, 16)
	for {
		select {
		case record := <-b.records:
			recordBatch = append(recordBatch, record)

		case <-ctx.Done():
			// TODO: handle record batch error (return error to all blocked
			// adders)
			err := b.persistRecordBatch(recordBatch)
			fmt.Printf("persistRecordBatch: %s\n", err)

			// Make waiting adders return
			close(b.blockAdd)

			b.mu.Lock()
			b.collectingBatch = false
			b.mu.Unlock()

			// TODO: handle ctx error
			return
		}
	}
}
