package recordbatch

import (
	"context"
	"sync"
)

type Batcher struct {
	recordBatch [][]byte

	mu              sync.Mutex
	collectingBatch bool

	makeContext func() context.Context
	records     chan []byte

	output chan [][]byte
}

func NewBatcher(makeContext func() context.Context) *Batcher {
	return &Batcher{
		recordBatch: make([][]byte, 0, 16),
		mu:          sync.Mutex{},
		records:     make(chan []byte),
		output:      make(chan [][]byte, 8),
		makeContext: makeContext,
	}
}

func (b *Batcher) Add(record []byte) error {
	b.mu.Lock()
	if !b.collectingBatch {
		ctx := b.makeContext()
		b.collectingBatch = true
		go b.collectBatch(ctx)
	}
	b.mu.Unlock()

	b.records <- record
	return nil
}

func (b *Batcher) collectBatch(ctx context.Context) {
	recordBatch := make([][]byte, 0, 16)
	for {
		select {
		case record := <-b.records:
			recordBatch = append(recordBatch, record)

		case <-ctx.Done():
			b.output <- recordBatch

			b.mu.Lock()
			b.collectingBatch = false
			b.mu.Unlock()

			// TODO: handle ctx error
			return
		}
	}
}

func (b *Batcher) Output() chan [][]byte {
	return b.output
}
