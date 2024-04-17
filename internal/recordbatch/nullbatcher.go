package recordbatch

// NullBatcher calls persistRecordBatch() for every record it receives. This is
// useful for testing.
type NullBatcher struct {
	persistRecordBatch func(RecordBatch) error
}

func NewNullBatcher(persistRecordBatch func(RecordBatch) error) *NullBatcher {
	return &NullBatcher{
		persistRecordBatch: persistRecordBatch,
	}
}

func (b *NullBatcher) AddRecord(r Record) error {
	return b.persistRecordBatch(RecordBatch{r})
}
