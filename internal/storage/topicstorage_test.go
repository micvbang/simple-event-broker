package storage_test

import (
	"context"
	"os"
	"testing"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

var log = logger.NewDefault(context.Background())

// TestStorageEmpty verifies that reading from an empty topic returns
// ErrOutOfBounds.
func TestStorageEmpty(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	s, err := storage.NewTopicStorage(log, storage.DiskStorage{}, tempDir, "mytopic")
	require.NoError(t, err)

	// Test
	_, err = s.ReadRecord(0)

	// Verify
	require.ErrorIs(t, err, storage.ErrOutOfBounds)
}

// TestStorageWriteRecordBatchSingleBatch verifies that all records from a
// single Record batch can be read back, and that reading out of bounds returns
// ErrOutOfBounds.
func TestStorageWriteRecordBatchSingleBatch(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	s, err := storage.NewTopicStorage(log, storage.DiskStorage{}, tempDir, "mytopic")
	require.NoError(t, err)

	recordBatch := tester.MakeRandomRecordBatch(5)

	// Test
	err = s.AddRecordBatch(recordBatch)
	require.NoError(t, err)

	// Verify
	for recordID, record := range recordBatch {
		got, err := s.ReadRecord(uint64(recordID))
		require.NoError(t, err)
		require.Equal(t, record, got)
	}

	// Out of bounds reads
	_, err = s.ReadRecord(uint64(len(recordBatch)))
	require.ErrorIs(t, err, storage.ErrOutOfBounds)

	_, err = s.ReadRecord(uint64(len(recordBatch) + 5))
	require.ErrorIs(t, err, storage.ErrOutOfBounds)
}

// TestStorageWriteRecordBatchMultipleBatches verifies that multiple
// RecordBatches can be written to the underlying storage and be read back
// again, and that reading beyond the number of existing records yields
// ErrOutOfBounds.
func TestStorageWriteRecordBatchMultipleBatches(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	s, err := storage.NewTopicStorage(log, storage.DiskStorage{}, tempDir, "mytopic")
	require.NoError(t, err)

	recordBatch1 := tester.MakeRandomRecordBatch(5)
	recordBatch2 := tester.MakeRandomRecordBatch(3)

	// Test
	err = s.AddRecordBatch(recordBatch1)
	require.NoError(t, err)

	err = s.AddRecordBatch(recordBatch2)
	require.NoError(t, err)

	// Verify
	for recordID, record := range append(recordBatch1, recordBatch2...) {
		got, err := s.ReadRecord(uint64(recordID))
		require.NoError(t, err)
		require.Equal(t, record, got)
	}

	// Out of bounds reads
	_, err = s.ReadRecord(uint64(len(recordBatch1) + len(recordBatch2)))
	require.ErrorIs(t, err, storage.ErrOutOfBounds)
}

// TestStorageOpenExistingStorage verifies that storage.Storage correctly
// initializes from a topic that already exists and has many data files.
func TestStorageOpenExistingStorage(t *testing.T) {
	const topicName = "my_topic"

	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	totalRecords := 0
	recordBatches := make([]recordbatch.RecordBatch, 50)
	for i := 0; i < len(recordBatches); i++ {
		batchSize := 1 + inty.RandomN(5)
		totalRecords += batchSize
		recordBatches[i] = tester.MakeRandomRecordBatch(batchSize)
	}

	{
		s1, err := storage.NewTopicStorage(log, storage.DiskStorage{}, tempDir, topicName)
		require.NoError(t, err)

		for _, recordBatch := range recordBatches {
			err = s1.AddRecordBatch(recordBatch)
			require.NoError(t, err)
		}
	}

	// Test
	s2, err := storage.NewTopicStorage(log, storage.DiskStorage{}, tempDir, topicName)
	require.NoError(t, err)

	// Verify
	recordID := 0
	for _, recordBatch := range recordBatches {
		for _, record := range recordBatch {
			got, err := s2.ReadRecord(uint64(recordID))
			require.NoError(t, err)
			require.Equal(t, record, got)

			recordID += 1
		}
	}

	// Out of bounds reads
	_, err = s2.ReadRecord(uint64(totalRecords + 1))
	require.ErrorIs(t, err, storage.ErrOutOfBounds)
}

// TestStorageOpenExistingStorage verifies that storage.Storage correctly
// initializes from a topic that already exists, and can correctly append
// records to it.
// NOTE: this is a regression test that handles an off by one error in
// NewTopicStorage().
func TestStorageOpenExistingStorageAndAppend(t *testing.T) {
	const topicName = "my_topic"

	tempDir, err := os.MkdirTemp("", "seb_*")
	require.NoError(t, err)

	recordBatch1 := tester.MakeRandomRecordBatch(1)
	{
		s1, err := storage.NewTopicStorage(log, storage.DiskStorage{}, tempDir, topicName)
		require.NoError(t, err)

		err = s1.AddRecordBatch(recordBatch1)
		require.NoError(t, err)
	}

	s2, err := storage.NewTopicStorage(log, storage.DiskStorage{}, tempDir, topicName)
	require.NoError(t, err)

	// Test
	recordBatch2 := tester.MakeRandomRecordBatch(1)
	err = s2.AddRecordBatch(recordBatch2)
	require.NoError(t, err)

	// Verify
	recordID := 0
	allRecords := append(recordBatch1, recordBatch2...)
	for _, record := range allRecords {
		got, err := s2.ReadRecord(uint64(recordID))
		require.NoError(t, err)
		require.Equal(t, record, got)

		recordID += 1
	}

	// Out of bounds reads
	_, err = s2.ReadRecord(uint64(len(allRecords)))
	require.ErrorIs(t, err, storage.ErrOutOfBounds)
}
