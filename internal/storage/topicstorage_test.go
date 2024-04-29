package storage_test

import (
	"context"
	"path"
	"testing"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

var (
	log = logger.NewDefault(context.Background())
)

// TestStorageEmpty verifies that reading from an empty topic returns
// ErrOutOfBounds.
func TestStorageEmpty(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, backingStorage storage.BackingStorage, cache *storage.Cache) {
		tempDir := tester.TempDir(t)

		s, err := storage.NewTopicStorage(log, backingStorage, tempDir, "mytopic", cache, nil)
		require.NoError(t, err)

		// Test
		_, err = s.ReadRecord(0)

		// Verify
		require.ErrorIs(t, err, storage.ErrOutOfBounds)
	})
}

// TestStorageWriteRecordBatchSingleBatch verifies that all records from a
// single Record batch can be read back, and that reading out of bounds returns
// ErrOutOfBounds.
func TestStorageWriteRecordBatchSingleBatch(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, backingStorage storage.BackingStorage, cache *storage.Cache) {
		tempDir := tester.TempDir(t)

		s, err := storage.NewTopicStorage(log, backingStorage, tempDir, "mytopic", cache, nil)
		require.NoError(t, err)

		const recordBatchSize = 5
		recordBatch := tester.MakeRandomRecordBatch(recordBatchSize)

		// Test
		recordIDs, err := s.AddRecordBatch(recordBatch)
		require.NoError(t, err)
		tester.RequireRecordIDs(t, 0, recordBatchSize, recordIDs)

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
	})
}

// TestStorageWriteRecordBatchMultipleBatches verifies that multiple
// RecordBatches can be written to the underlying storage and be read back
// again, and that reading beyond the number of existing records yields
// ErrOutOfBounds.
func TestStorageWriteRecordBatchMultipleBatches(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, backingStorage storage.BackingStorage, cache *storage.Cache) {
		tempDir := tester.TempDir(t)

		s, err := storage.NewTopicStorage(log, backingStorage, tempDir, "mytopic", cache, nil)
		require.NoError(t, err)

		recordBatch1 := tester.MakeRandomRecordBatch(5)
		recordBatch2 := tester.MakeRandomRecordBatch(3)

		// Test
		b1RecordIDs, err := s.AddRecordBatch(recordBatch1)
		require.NoError(t, err)
		tester.RequireRecordIDs(t, 0, 5, b1RecordIDs)

		b2RecordIDs, err := s.AddRecordBatch(recordBatch2)
		require.NoError(t, err)
		tester.RequireRecordIDs(t, 5, 8, b2RecordIDs)

		// Verify
		for recordID, record := range append(recordBatch1, recordBatch2...) {
			got, err := s.ReadRecord(uint64(recordID))
			require.NoError(t, err)
			require.Equal(t, record, got)
		}

		// Out of bounds reads
		_, err = s.ReadRecord(uint64(len(recordBatch1) + len(recordBatch2)))
		require.ErrorIs(t, err, storage.ErrOutOfBounds)
	})
}

// TestStorageOpenExistingStorage verifies that storage.Storage correctly
// initializes from a topic that already exists and has many data files.
func TestStorageOpenExistingStorage(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, backingStorage storage.BackingStorage, cache *storage.Cache) {
		const topicName = "my_topic"

		tempDir := tester.TempDir(t)

		totalRecords := 0
		recordBatches := make([]recordbatch.RecordBatch, 50)
		for i := 0; i < len(recordBatches); i++ {
			batchSize := 1 + inty.RandomN(5)
			totalRecords += batchSize
			recordBatches[i] = tester.MakeRandomRecordBatch(batchSize)
		}

		{
			cache, err := storage.NewCache(log, storage.NewMemoryCache(log))
			require.NoError(t, err)
			s1, err := storage.NewTopicStorage(log, backingStorage, tempDir, topicName, cache, nil)
			require.NoError(t, err)

			batchStartID := uint64(0)
			for _, recordBatch := range recordBatches {
				batchEndID := batchStartID + uint64(len(recordBatch))

				recordIDs, err := s1.AddRecordBatch(recordBatch)
				require.NoError(t, err)
				tester.RequireRecordIDs(t, batchStartID, batchEndID, recordIDs)

				batchStartID += uint64(len(recordBatch))
			}
		}

		// Test
		s2, err := storage.NewTopicStorage(log, backingStorage, tempDir, topicName, cache, nil)
		require.NoError(t, err)

		// Verify
		recordID := 0
		for _, recordBatch := range recordBatches {
			for _, expected := range recordBatch {
				got, err := s2.ReadRecord(uint64(recordID))
				require.NoError(t, err)
				require.Equal(t, expected, got)

				recordID += 1
			}
		}

		// Out of bounds reads
		_, err = s2.ReadRecord(uint64(totalRecords + 1))
		require.ErrorIs(t, err, storage.ErrOutOfBounds)
	})
}

// TestStorageOpenExistingStorage verifies that storage.Storage correctly
// initializes from a topic that already exists, and can correctly append
// records to it.
// NOTE: this is a regression test that handles an off by one error in
// NewTopicStorage().
func TestStorageOpenExistingStorageAndAppend(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, backingStorage storage.BackingStorage, cache *storage.Cache) {
		const topicName = "my_topic"

		tempDir := tester.TempDir(t)

		recordBatch1 := tester.MakeRandomRecordBatch(1)
		{
			cache, err := storage.NewCache(log, storage.NewMemoryCache(log))
			require.NoError(t, err)
			s1, err := storage.NewTopicStorage(log, backingStorage, tempDir, topicName, cache, nil)
			require.NoError(t, err)

			recordIDs, err := s1.AddRecordBatch(recordBatch1)
			require.NoError(t, err)
			tester.RequireRecordIDs(t, 0, 1, recordIDs)
		}

		s2, err := storage.NewTopicStorage(log, backingStorage, tempDir, topicName, cache, nil)
		require.NoError(t, err)

		// Test
		recordBatch2 := tester.MakeRandomRecordBatch(1)
		recordIDs, err := s2.AddRecordBatch(recordBatch2)
		require.NoError(t, err)
		tester.RequireRecordIDs(t, 1, 2, recordIDs)

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
	})
}

// TestStorageCacheWrite verifies that AddRecordBatch uses the cache to cache
// the record batch.
func TestStorageCacheWrite(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, backingStorage storage.BackingStorage, cache *storage.Cache) {
		const topicName = "my_topic"

		storageDir := tester.TempDir(t)

		cacheStorage := storage.NewMemoryCache(log)
		cache, err := storage.NewCache(log, cacheStorage)
		require.NoError(t, err)

		s, err := storage.NewTopicStorage(log, backingStorage, storageDir, topicName, cache, nil)
		require.NoError(t, err)

		expectedStorageDir := getStorageKey(storageDir, topicName, 0)
		const recordBatchSize = 5
		expectedRecordBatch := tester.MakeRandomRecordBatch(recordBatchSize)

		// Act
		recordIDs, err := s.AddRecordBatch(expectedRecordBatch)
		require.NoError(t, err)
		tester.RequireRecordIDs(t, 0, recordBatchSize, recordIDs)

		// Assert

		// record batch must be written to both backing storage and cache.
		_, err = cache.Reader(expectedStorageDir)
		require.NoError(t, err)

		_, err = backingStorage.Reader(expectedStorageDir)
		require.NoError(t, err)

		for recordID, expected := range expectedRecordBatch {
			got, err := s.ReadRecord(uint64(recordID))
			require.NoError(t, err)
			require.Equal(t, expected, got)
		}
	})
}

// TestStorageCacheWrite verifies that ReadRecord uses the cache to read
// results.
func TestStorageCacheReadFromCache(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, backingStorage storage.BackingStorage, cache *storage.Cache) {
		const topicName = "my_topic"

		storageDir := tester.TempDir(t)

		s, err := storage.NewTopicStorage(log, backingStorage, storageDir, topicName, cache, nil)
		require.NoError(t, err)

		const recordBatchSize = 5
		expectedRecordBatch := tester.MakeRandomRecordBatch(recordBatchSize)
		recordIDs, err := s.AddRecordBatch(expectedRecordBatch)
		require.NoError(t, err)
		tester.RequireRecordIDs(t, 0, recordBatchSize, recordIDs)

		// NOTE: in order to prove that we're reading from the cache and not from the
		// backing storage, we're making the file in the backing storage zero bytes long.
		wtr, err := backingStorage.Writer(getStorageKey(storageDir, topicName, 0))
		require.NoError(t, err)
		tester.WriteAndClose(t, wtr, []byte{})

		for recordID, expected := range expectedRecordBatch {
			// Act
			got, err := s.ReadRecord(uint64(recordID))

			// Assert
			require.NoError(t, err)
			require.Equal(t, expected, got)
		}
	})
}

// TestStorageCacheReadFileNotInCache verifies that ReadRecord can fetch record
// batches from the backing storage if it's not in the cache.
func TestStorageCacheReadFileNotInCache(t *testing.T) {
	tester.TestBackingStorage(t, func(t *testing.T, backingStorage storage.BackingStorage) {
		const topicName = "my_topic"

		storageDir := tester.TempDir(t)

		cacheStorage := storage.NewMemoryCache(log)
		cache, err := storage.NewCache(log, cacheStorage)
		require.NoError(t, err)

		s, err := storage.NewTopicStorage(log, backingStorage, storageDir, topicName, cache, nil)
		require.NoError(t, err)

		const recordBatchSize = 5
		expectedRecordBatch := tester.MakeRandomRecordBatch(recordBatchSize)
		recordIDs, err := s.AddRecordBatch(expectedRecordBatch)
		require.NoError(t, err)
		tester.RequireRecordIDs(t, 0, recordBatchSize, recordIDs)

		// NOTE: in order to prove that we're reading from the backing storage and
		// not from the cache, we're removing the file from the cache.
		err = cacheStorage.Remove(getStorageKey(storageDir, topicName, 0))
		require.NoError(t, err)

		for recordID, expected := range expectedRecordBatch {
			// Act
			got, err := s.ReadRecord(uint64(recordID))

			// Assert
			require.NoError(t, err)
			require.Equal(t, expected, got)
		}
	})
}

// TestStorageCompressFiles verifies that TopicStorage uses the given Compressor
// to seemlessly compresses and decompresses files when they're written to the
// backing storage.
func TestStorageCompressFiles(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, backingStorage storage.BackingStorage, cache *storage.Cache) {
		const topicName = "topicName"
		storageDir := tester.TempDir(t)

		compressor := storage.Gzip{}
		s, err := storage.NewTopicStorage(log, backingStorage, storageDir, topicName, cache, compressor)
		require.NoError(t, err)

		const recordBatchSize = 5
		expectedRecordBatch := tester.MakeRandomRecordBatch(recordBatchSize)
		recordIDs, err := s.AddRecordBatch(expectedRecordBatch)
		require.NoError(t, err)
		tester.RequireRecordIDs(t, 0, recordBatchSize, recordIDs)

		backingStorageReader, err := backingStorage.Reader(getStorageKey(storageDir, topicName, 0))
		require.NoError(t, err)

		// read records directly from compressor in order to prove that they're compressed
		compressorReader, err := compressor.NewReader(backingStorageReader)
		require.NoError(t, err)

		buf := tester.ReadToMemory(t, compressorReader)

		parser, err := recordbatch.Parse(buf)
		require.NoError(t, err)
		require.Equal(t, uint32(len(expectedRecordBatch)), parser.Header.NumRecords)

		// can read records from compressed data
		for recordID, expected := range expectedRecordBatch {
			// Act
			got, err := s.ReadRecord(uint64(recordID))

			// Assert
			require.NoError(t, err)
			require.Equal(t, expected, got)
		}
	})
}

// TestTopicStorageEndRecordID verifies that EndRecordID returns the record id
// of the next record that is added, i.e. the id of most-recently-added+1.
func TestTopicStorageEndRecordID(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, backingStorage storage.BackingStorage, cache *storage.Cache) {
		storageDir := tester.TempDir(t)
		const topicName = "topicName"
		compressor := storage.Gzip{}
		s, err := storage.NewTopicStorage(log, backingStorage, storageDir, topicName, cache, compressor)
		require.NoError(t, err)

		// no record added yet, next id should be 0
		recordID := s.EndRecordID()
		require.Equal(t, uint64(0), recordID)

		nextRecordID := uint64(0)
		for range 10 {
			recordBatch := tester.MakeRandomRecordBatch(1 + inty.RandomN(10))
			recordIDs, err := s.AddRecordBatch(recordBatch)
			require.NoError(t, err)
			tester.RequireRecordIDs(t, nextRecordID, nextRecordID+uint64(len(recordBatch)), recordIDs)

			nextRecordID += uint64(len(recordBatch))

			// Act, Assert
			recordID := s.EndRecordID()
			require.Equal(t, nextRecordID, recordID)
		}
	})
}

func getStorageKey(storageDir string, topicName string, recordID uint64) string {
	return path.Join(storageDir, storage.RecordBatchPath(topicName, recordID))
}
