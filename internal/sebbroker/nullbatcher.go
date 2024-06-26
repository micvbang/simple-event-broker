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

func (b *nullBatcher) AddRecords(records []sebrecords.Record) ([]uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	offsets, err := b.persist(records)
	if err != nil {
		return nil, err
	}

	if len(offsets) != len(records) {
		// This is not supposed to happen; if it does, we can't trust b.persist()
		panic(fmt.Sprintf("unexpected number of offsets returned %d, expected %d", len(offsets), len(records)))
	}

	return offsets, nil
}

func (b *nullBatcher) AddRecord(record sebrecords.Record) (uint64, error) {
	offsets, err := b.AddRecords([]sebrecords.Record{record})
	if err != nil {
		return 0, err
	}

	if len(offsets) != 1 {
		// This is not supposed to happen; if it does, we can't trust b.persist()
		panic(fmt.Sprintf("unexpected number of offsets returned: %d", len(offsets)))
	}

	return offsets[0], nil

}
