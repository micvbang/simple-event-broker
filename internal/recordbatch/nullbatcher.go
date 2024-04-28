package recordbatch

import "fmt"

// NullBatcher calls persist() for every record it receives, always creating a
// record batch of size 1. This is useful for testing.
type NullBatcher struct {
	persist Persist
}

func NewNullBatcher(persist Persist) *NullBatcher {
	return &NullBatcher{
		persist: persist,
	}
}

func (b *NullBatcher) AddRecord(r Record) (uint64, error) {
	recordIDs, err := b.persist(RecordBatch{r})
	if err != nil {
		return 0, err
	}

	if len(recordIDs) != 1 {
		return 0, fmt.Errorf("unexpected number of records %d", len(recordIDs))
	}

	return recordIDs[0], nil
}
