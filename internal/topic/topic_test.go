package topic_test

import (
	"context"
	"testing"
	"time"

	"github.com/micvbang/go-helpy/inty"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/cache"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/topic"
	"github.com/stretchr/testify/require"
)

var (
	log = logger.NewDefault(context.Background())
)

// TestStorageEmpty verifies that reading from an empty topic returns
// ErrOutOfBounds.
func TestStorageEmpty(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage topic.Storage, cache *cache.Cache) {
		s, err := topic.New(log, backingStorage, "mytopic", cache, nil)
		require.NoError(t, err)

		// Test
		_, err = s.ReadRecord(0)

		// Verify
		require.ErrorIs(t, err, seb.ErrOutOfBounds)
	})
}

// TestStorageWriteRecordBatchSingleBatch verifies that all records from a
// single Record batch can be read back, and that reading out of bounds returns
// ErrOutOfBounds.
func TestStorageWriteRecordBatchSingleBatch(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage topic.Storage, cache *cache.Cache) {
		s, err := topic.New(log, backingStorage, "mytopic", cache, nil)
		require.NoError(t, err)

		const recordBatchSize = 5
		recordBatch := tester.MakeRandomRecordBatch(recordBatchSize)

		// Test
		offsets, err := s.AddRecordBatch(recordBatch)
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, recordBatchSize, offsets)

		// Verify
		for offset, record := range recordBatch {
			got, err := s.ReadRecord(uint64(offset))
			require.NoError(t, err)
			require.Equal(t, record, got)
		}

		// Out of bounds reads
		_, err = s.ReadRecord(uint64(len(recordBatch)))
		require.ErrorIs(t, err, seb.ErrOutOfBounds)

		_, err = s.ReadRecord(uint64(len(recordBatch) + 5))
		require.ErrorIs(t, err, seb.ErrOutOfBounds)
	})
}

// TestStorageWriteRecordBatchMultipleBatches verifies that multiple
// RecordBatches can be written to the underlying storage and be read back
// again, and that reading beyond the number of existing records yields
// ErrOutOfBounds.
func TestStorageWriteRecordBatchMultipleBatches(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage topic.Storage, cache *cache.Cache) {
		s, err := topic.New(log, backingStorage, "mytopic", cache, nil)
		require.NoError(t, err)

		recordBatch1 := tester.MakeRandomRecordBatch(5)
		recordBatch2 := tester.MakeRandomRecordBatch(3)

		// Test
		b1Offsets, err := s.AddRecordBatch(recordBatch1)
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, 5, b1Offsets)

		b2Offsets, err := s.AddRecordBatch(recordBatch2)
		require.NoError(t, err)
		tester.RequireOffsets(t, 5, 8, b2Offsets)

		// Verify
		for offset, record := range append(recordBatch1, recordBatch2...) {
			got, err := s.ReadRecord(uint64(offset))
			require.NoError(t, err)
			require.Equal(t, record, got)
		}

		// Out of bounds reads
		_, err = s.ReadRecord(uint64(len(recordBatch1) + len(recordBatch2)))
		require.ErrorIs(t, err, seb.ErrOutOfBounds)
	})
}

// TestStorageOpenExistingStorage verifies that storage.Storage correctly
// initializes from a topic that already exists and has many data files.
func TestStorageOpenExistingStorage(t *testing.T) {
	tester.TestBackingStorage(t, func(t *testing.T, backingStorage topic.Storage) {
		const topicName = "my_topic"

		totalRecords := 0
		recordBatches := make([]recordbatch.RecordBatch, 50)
		for i := 0; i < len(recordBatches); i++ {
			batchSize := 1 + inty.RandomN(5)
			totalRecords += batchSize
			recordBatches[i] = tester.MakeRandomRecordBatch(batchSize)
		}

		{
			cache, err := cache.New(log, cache.NewMemoryStorage(log))
			require.NoError(t, err)
			s1, err := topic.New(log, backingStorage, topicName, cache, nil)
			require.NoError(t, err)

			batchStartID := uint64(0)
			for _, recordBatch := range recordBatches {
				batchEndID := batchStartID + uint64(len(recordBatch))

				offsets, err := s1.AddRecordBatch(recordBatch)
				require.NoError(t, err)
				tester.RequireOffsets(t, batchStartID, batchEndID, offsets)

				batchStartID += uint64(len(recordBatch))
			}
		}

		cache, err := cache.New(log, cache.NewMemoryStorage(log))
		require.NoError(t, err)

		// Test
		s2, err := topic.New(log, backingStorage, topicName, cache, nil)
		require.NoError(t, err)

		// Verify
		offset := 0
		for _, recordBatch := range recordBatches {
			for _, expected := range recordBatch {
				got, err := s2.ReadRecord(uint64(offset))
				require.NoError(t, err)
				require.Equal(t, expected, got)

				offset += 1
			}
		}

		// Out of bounds reads
		_, err = s2.ReadRecord(uint64(totalRecords + 1))
		require.ErrorIs(t, err, seb.ErrOutOfBounds)
	})
}

// TestStorageOpenExistingStorage verifies that storage.Storage correctly
// initializes from a topic that already exists, and can correctly append
// records to it.
// NOTE: this is a regression test that handles an off by one error in
// NewTopic().
func TestStorageOpenExistingStorageAndAppend(t *testing.T) {
	tester.TestBackingStorage(t, func(t *testing.T, topicStorage topic.Storage) {
		const topicName = "my_topic"

		recordBatch1 := tester.MakeRandomRecordBatch(1)
		{
			cache, err := cache.New(log, cache.NewMemoryStorage(log))
			require.NoError(t, err)
			s1, err := topic.New(log, topicStorage, topicName, cache, nil)
			require.NoError(t, err)

			offsets, err := s1.AddRecordBatch(recordBatch1)
			require.NoError(t, err)
			tester.RequireOffsets(t, 0, 1, offsets)
		}

		cache, err := cache.New(log, cache.NewMemoryStorage(log))
		require.NoError(t, err)

		s2, err := topic.New(log, topicStorage, topicName, cache, nil)
		require.NoError(t, err)

		// Test
		recordBatch2 := tester.MakeRandomRecordBatch(1)
		offsets, err := s2.AddRecordBatch(recordBatch2)
		require.NoError(t, err)
		tester.RequireOffsets(t, 1, 2, offsets)

		// Verify
		offset := 0
		allRecords := append(recordBatch1, recordBatch2...)
		for _, record := range allRecords {
			got, err := s2.ReadRecord(uint64(offset))
			require.NoError(t, err)
			require.Equal(t, record, got)

			offset += 1
		}

		// Out of bounds reads
		_, err = s2.ReadRecord(uint64(len(allRecords)))
		require.ErrorIs(t, err, seb.ErrOutOfBounds)
	})
}

// TestStorageCacheWrite verifies that AddRecordBatch uses the cache to cache
// the record batch.
func TestStorageCacheWrite(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage topic.Storage, cache *cache.Cache) {
		const topicName = "my_topic"

		s, err := topic.New(log, backingStorage, topicName, cache, nil)
		require.NoError(t, err)

		recordBatchKey := topic.RecordBatchKey(topicName, 0)
		const recordBatchSize = 5
		expectedRecordBatch := tester.MakeRandomRecordBatch(recordBatchSize)

		// Act
		offsets, err := s.AddRecordBatch(expectedRecordBatch)
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, recordBatchSize, offsets)

		// Assert

		// record batch must be written to both backing storage and cache.
		_, err = cache.Reader(recordBatchKey)
		require.NoError(t, err)

		_, err = backingStorage.Reader(recordBatchKey)
		require.NoError(t, err)

		for offset, expected := range expectedRecordBatch {
			got, err := s.ReadRecord(uint64(offset))
			require.NoError(t, err)
			require.Equal(t, expected, got)
		}
	})
}

// TestStorageCacheWrite verifies that ReadRecord uses the cache to read
// results.
func TestStorageCacheReadFromCache(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage topic.Storage, cache *cache.Cache) {
		const topicName = "my_topic"

		s, err := topic.New(log, backingStorage, topicName, cache, nil)
		require.NoError(t, err)

		const recordBatchSize = 5
		expectedRecordBatch := tester.MakeRandomRecordBatch(recordBatchSize)
		offsets, err := s.AddRecordBatch(expectedRecordBatch)
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, recordBatchSize, offsets)

		// NOTE: in order to prove that we're reading from the cache and not
		// from the backing storage, we're truncating the file in the backing
		// storage to zero bytes.
		wtr, err := backingStorage.Writer(topic.RecordBatchKey(topicName, 0))
		require.NoError(t, err)
		tester.WriteAndClose(t, wtr, []byte{})

		for offset, expected := range expectedRecordBatch {
			// Act
			got, err := s.ReadRecord(uint64(offset))

			// Assert
			require.NoError(t, err)
			require.Equal(t, expected, got)
		}
	})
}

// TestStorageCacheReadFileNotInCache verifies that ReadRecord can fetch record
// batches from the backing storage if it's not in the cache.
func TestStorageCacheReadFileNotInCache(t *testing.T) {
	tester.TestBackingStorage(t, func(t *testing.T, backingStorage topic.Storage) {
		const topicName = "my_topic"

		cacheStorage := cache.NewMemoryStorage(log)
		cache, err := cache.New(log, cacheStorage)
		require.NoError(t, err)

		s, err := topic.New(log, backingStorage, topicName, cache, nil)
		require.NoError(t, err)

		const recordBatchSize = 5
		expectedRecordBatch := tester.MakeRandomRecordBatch(recordBatchSize)
		offsets, err := s.AddRecordBatch(expectedRecordBatch)
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, recordBatchSize, offsets)

		// NOTE: in order to prove that we're reading from the backing storage and
		// not from the cache, we're removing the file from the cache.
		err = cacheStorage.Remove(topic.RecordBatchKey(topicName, 0))
		require.NoError(t, err)

		for offset, expected := range expectedRecordBatch {
			// Act
			got, err := s.ReadRecord(uint64(offset))

			// Assert
			require.NoError(t, err)
			require.Equal(t, expected, got)
		}
	})
}

// TestStorageCompressFiles verifies that Topic uses the given Compressor
// to seemlessly compresses and decompresses files when they're written to the
// backing storage.
func TestStorageCompressFiles(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage topic.Storage, cache *cache.Cache) {
		const topicName = "topicName"
		compressor := topic.Gzip{}
		s, err := topic.New(log, backingStorage, topicName, cache, compressor)
		require.NoError(t, err)

		const recordBatchSize = 5
		expectedRecordBatch := tester.MakeRandomRecordBatch(recordBatchSize)
		offsets, err := s.AddRecordBatch(expectedRecordBatch)
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, recordBatchSize, offsets)

		backingStorageReader, err := backingStorage.Reader(topic.RecordBatchKey(topicName, 0))
		require.NoError(t, err)

		// read records directly from compressor in order to prove that they're compressed
		compressorReader, err := compressor.NewReader(backingStorageReader)
		require.NoError(t, err)

		buf := tester.ReadToMemory(t, compressorReader)

		parser, err := recordbatch.Parse(buf)
		require.NoError(t, err)
		require.Equal(t, uint32(len(expectedRecordBatch)), parser.Header.NumRecords)

		// can read records from compressed data
		for offset, expected := range expectedRecordBatch {
			// Act
			got, err := s.ReadRecord(uint64(offset))

			// Assert
			require.NoError(t, err)
			require.Equal(t, expected, got)
		}
	})
}

// TestTopicEndOffset verifies that EndOffset returns the offset of the
// next record that is added, i.e. the id of most-recently-added+1.
func TestTopicEndOffset(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage topic.Storage, cache *cache.Cache) {
		const topicName = "topicName"
		s, err := topic.New(log, backingStorage, topicName, cache, topic.Gzip{})
		require.NoError(t, err)

		// no record added yet, next id should be 0
		offset := s.NextOffset()
		require.Equal(t, uint64(0), offset)

		nextOffset := uint64(0)
		for range 10 {
			recordBatch := tester.MakeRandomRecordBatch(1 + inty.RandomN(10))
			offsets, err := s.AddRecordBatch(recordBatch)
			require.NoError(t, err)
			tester.RequireOffsets(t, nextOffset, nextOffset+uint64(len(recordBatch)), offsets)

			nextOffset += uint64(len(recordBatch))

			// Act, Assert
			offset := s.NextOffset()
			require.Equal(t, nextOffset, offset)
		}
	})
}

// TestTopicOffsetCond verifies that Topic.OffsetCond.Wait() blocks until the
// expected offset has been reached.
func TestTopicOffsetCond(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage topic.Storage, cache *cache.Cache) {
		const topicName = "topicName"
		s, err := topic.New(log, backingStorage, topicName, cache, topic.Gzip{})
		require.NoError(t, err)

		ctx := context.Background()
		returned := make(chan struct{})
		go func() {
			s.OffsetCond.Wait(ctx, 100)
			close(returned)
		}()

		// Wait() is waiting for offset 100, this will add offset 0
		_, err = s.AddRecordBatch(tester.MakeRandomRecordBatch(1))
		require.NoError(t, err)

		time.Sleep(25 * time.Millisecond)
		select {
		case <-returned:
			t.Fatalf("did not expect wait to have returned")
		default:
		}

		// Act
		_, err = s.AddRecordBatch(tester.MakeRandomRecordBatch(100))
		require.NoError(t, err)

		// Assert
		// (test will time out if returned isn't closed)
		<-returned
	})
}

// TestTopicOffsetCondContextExpired verifies that Topic.OffsetCond.Wait()
// returns when the given context expires.
func TestTopicOffsetCondContextExpired(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage topic.Storage, cache *cache.Cache) {
		const topicName = "topicName"
		s, err := topic.New(log, backingStorage, topicName, cache, topic.Gzip{})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		returned := make(chan struct{})
		go func() {
			s.OffsetCond.Wait(ctx, 1)
			close(returned)
		}()

		time.Sleep(25 * time.Millisecond)
		select {
		case <-returned:
			t.Fatalf("did not expect wait to have returned")
		default:
		}

		// Act
		cancel()

		// Assert
		// (test will time out if returned isn't closed)
		<-returned
	})
}