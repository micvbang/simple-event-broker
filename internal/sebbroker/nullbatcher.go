package sebbroker

import (
	"fmt"
	"sync"

	"github.com/micvbang/simple-event-broker/internal/sebrecords"
)

// nullBatcher calls persist() for every record it receives, always creating a
// record batch of size 1. This is useful for testing.
type nullBatcher struct {
	mu      sync.Mutex
	persist Persist
}

func NewNullBatcher(persist Persist) *nullBatcher {
	return &nullBatcher{
		persist: persist,
	}
}

func (b *nullBatcher) AddRecords(batch sebrecords.Batch) ([]uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	offsets, err := b.persist(batch)
	if err != nil {
		return nil, err
	}

	if len(offsets) != batch.Len() {
		// This is not supposed to happen; if it does, we can't trust b.persist()
		panic(fmt.Sprintf("unexpected number of offsets returned %d, expected %d", len(offsets), batch.Len()))
	}

	return offsets, nil
}
