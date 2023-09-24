package storage_test

import (
	"os"
	"testing"

	"github.com/micvbang/simple-commit-log/internal/storage"
	"github.com/stretchr/testify/require"
)

// TestDiskEmpty verifies that reading from an empty topic returns
// ErrOutOfBounds.
func TestDiskEmpty(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "scl_*")
	require.NoError(t, err)

	ds, err := storage.NewDiskStorage(tempDir, "my_topic")
	require.NoError(t, err)

	// Test
	_, err = ds.ReadRecord(0)

	// Verify
	require.ErrorIs(t, err, storage.ErrOutOfBounds)
}

// TestDiskWriteRecordBatchSingleBatch verifies that all records from a single
// Record batch can be read back, and that reading out of bounds returns
// ErrOutOfBounds.
func TestDiskWriteRecordBatchSingleBatch(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "scl_*")
	require.NoError(t, err)

	ds, err := storage.NewDiskStorage(tempDir, "my_topic")
	require.NoError(t, err)

	recordBatch := [][]byte{
		[]byte("hello mister topic"),
		[]byte("what a fine day today!"),
		[]byte("enjoy!"),
	}

	// Test
	err = ds.AddRecordBatch(recordBatch)
	require.NoError(t, err)

	// Verify
	for recordID, record := range recordBatch {
		got, err := ds.ReadRecord(uint64(recordID))
		require.NoError(t, err)
		require.Equal(t, record, got)
	}

	// Out of bounds reads
	_, err = ds.ReadRecord(uint64(len(recordBatch)))
	require.ErrorIs(t, err, storage.ErrOutOfBounds)

	_, err = ds.ReadRecord(uint64(len(recordBatch) + 5))
	require.ErrorIs(t, err, storage.ErrOutOfBounds)
}

// TestDiskWriteRecordBatchMultipleBatches verifies that multiple RecordBatches
// can be written to DiskStorage and read back again, and that reading beyond
// the number of existing records yields ErrOutOfBounds.
func TestDiskWriteRecordBatchMultipleBatches(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "scl_*")
	require.NoError(t, err)

	ds, err := storage.NewDiskStorage(tempDir, "my_topic")
	require.NoError(t, err)

	recordBatch1 := [][]byte{
		[]byte("hello mister topic"),
		[]byte("what a fine day today!"),
		[]byte("enjoy!"),
	}
	recordBatch2 := [][]byte{
		[]byte("second batch today!"),
		[]byte("let's goooooo"),
	}

	// Test
	err = ds.AddRecordBatch(recordBatch1)
	require.NoError(t, err)
	err = ds.AddRecordBatch(recordBatch2)
	require.NoError(t, err)

	// Verify
	for recordID, record := range append(recordBatch1, recordBatch2...) {
		got, err := ds.ReadRecord(uint64(recordID))

		require.NoError(t, err)
		require.Equal(t, record, got)
	}

	// Out of bounds reads
	_, err = ds.ReadRecord(uint64(len(recordBatch1) + len(recordBatch2)))
	require.ErrorIs(t, err, storage.ErrOutOfBounds)
}
