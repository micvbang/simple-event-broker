package sebtopic_test

import (
	"context"
	"testing"
	"time"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/slicey"
	"github.com/micvbang/go-helpy/timey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/cache"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/stretchr/testify/require"
)

var (
	log = logger.NewWithLevel(context.Background(), logger.LevelWarn)
)

// TestStorageEmpty verifies that reading from an empty topic returns
// ErrOutOfBounds.
func TestStorageEmpty(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *cache.Cache) {
		s, err := sebtopic.New(log, backingStorage, "mytopic", cache, sebtopic.WithCompress(nil))
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
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *cache.Cache) {
		s, err := sebtopic.New(log, backingStorage, "mytopic", cache, sebtopic.WithCompress(nil))
		require.NoError(t, err)

		const numRecords = 5
		records := tester.MakeRandomRecords(numRecords)

		// Test
		offsets, err := s.AddRecords(records)
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, numRecords, offsets)

		// Verify
		for offset, record := range records {
			got, err := s.ReadRecord(uint64(offset))
			require.NoError(t, err)
			require.Equal(t, record, got)
		}

		// Out of bounds reads
		_, err = s.ReadRecord(uint64(len(records)))
		require.ErrorIs(t, err, seb.ErrOutOfBounds)

		_, err = s.ReadRecord(uint64(len(records) + 5))
		require.ErrorIs(t, err, seb.ErrOutOfBounds)
	})
}

// TestStorageWriteRecordBatchMultipleBatches verifies that multiple
// RecordBatches can be written to the underlying storage and be read back
// again, and that reading beyond the number of existing records yields
// ErrOutOfBounds.
func TestStorageWriteRecordBatchMultipleBatches(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *cache.Cache) {
		s, err := sebtopic.New(log, backingStorage, "mytopic", cache)
		require.NoError(t, err)

		records1 := tester.MakeRandomRecords(5)
		records2 := tester.MakeRandomRecords(3)

		// Test
		b1Offsets, err := s.AddRecords(records1)
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, 5, b1Offsets)

		b2Offsets, err := s.AddRecords(records2)
		require.NoError(t, err)
		tester.RequireOffsets(t, 5, 8, b2Offsets)

		// Verify
		for offset, record := range append(records1, records2...) {
			got, err := s.ReadRecord(uint64(offset))
			require.NoError(t, err)
			require.Equal(t, record, got)
		}

		// Out of bounds reads
		_, err = s.ReadRecord(uint64(len(records1) + len(records2)))
		require.ErrorIs(t, err, seb.ErrOutOfBounds)
	})
}

// TestStorageOpenExistingStorage verifies that storage.Storage correctly
// initializes from a topic that already exists and has many data files.
func TestStorageOpenExistingStorage(t *testing.T) {
	tester.TestBackingStorage(t, func(t *testing.T, backingStorage sebtopic.Storage) {
		const topicName = "my_topic"

		totalRecords := 0
		recordsBatch := make([][]sebrecords.Record, 50)
		for i := 0; i < len(recordsBatch); i++ {
			batchSize := 1 + inty.RandomN(5)
			totalRecords += batchSize
			recordsBatch[i] = tester.MakeRandomRecords(batchSize)
		}

		{
			cache, err := cache.New(log, cache.NewMemoryStorage(log))
			require.NoError(t, err)
			s1, err := sebtopic.New(log, backingStorage, topicName, cache)
			require.NoError(t, err)

			batchStartID := uint64(0)
			for _, records := range recordsBatch {
				batchEndID := batchStartID + uint64(len(records))

				offsets, err := s1.AddRecords(records)
				require.NoError(t, err)
				tester.RequireOffsets(t, batchStartID, batchEndID, offsets)

				batchStartID += uint64(len(records))
			}
		}

		cache, err := cache.New(log, cache.NewMemoryStorage(log))
		require.NoError(t, err)

		// Test
		s2, err := sebtopic.New(log, backingStorage, topicName, cache)
		require.NoError(t, err)

		// Verify
		offset := 0
		for _, records := range recordsBatch {
			for _, expected := range records {
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
	tester.TestBackingStorage(t, func(t *testing.T, topicStorage sebtopic.Storage) {
		const topicName = "my_topic"

		records1 := tester.MakeRandomRecords(1)
		{
			cache, err := cache.New(log, cache.NewMemoryStorage(log))
			require.NoError(t, err)
			s1, err := sebtopic.New(log, topicStorage, topicName, cache)
			require.NoError(t, err)

			offsets, err := s1.AddRecords(records1)
			require.NoError(t, err)
			tester.RequireOffsets(t, 0, 1, offsets)
		}

		cache, err := cache.New(log, cache.NewMemoryStorage(log))
		require.NoError(t, err)

		s2, err := sebtopic.New(log, topicStorage, topicName, cache)
		require.NoError(t, err)

		// Test
		records2 := tester.MakeRandomRecords(1)
		offsets, err := s2.AddRecords(records2)
		require.NoError(t, err)
		tester.RequireOffsets(t, 1, 2, offsets)

		// Verify
		offset := 0
		allRecords := append(records1, records2...)
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
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *cache.Cache) {
		const topicName = "my_topic"

		s, err := sebtopic.New(log, backingStorage, topicName, cache)
		require.NoError(t, err)

		batchKey := sebtopic.RecordBatchKey(topicName, 0)
		const numRecords = 5
		expectedRecordBatch := tester.MakeRandomRecords(numRecords)

		// Act
		offsets, err := s.AddRecords(expectedRecordBatch)
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, numRecords, offsets)

		// Assert

		// record batch must be written to both backing storage and cache.
		_, err = cache.Reader(batchKey)
		require.NoError(t, err)

		_, err = backingStorage.Reader(batchKey)
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
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *cache.Cache) {
		const topicName = "my_topic"

		s, err := sebtopic.New(log, backingStorage, topicName, cache)
		require.NoError(t, err)

		const numRecords = 5
		expectedRecordBatch := tester.MakeRandomRecords(numRecords)
		offsets, err := s.AddRecords(expectedRecordBatch)
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, numRecords, offsets)

		// NOTE: in order to prove that we're reading from the cache and not
		// from the backing storage, we're truncating the file in the backing
		// storage to zero bytes.
		wtr, err := backingStorage.Writer(sebtopic.RecordBatchKey(topicName, 0))
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
	tester.TestBackingStorage(t, func(t *testing.T, backingStorage sebtopic.Storage) {
		const topicName = "my_topic"

		cacheStorage := cache.NewMemoryStorage(log)
		cache, err := cache.New(log, cacheStorage)
		require.NoError(t, err)

		s, err := sebtopic.New(log, backingStorage, topicName, cache)
		require.NoError(t, err)

		const numRecords = 5
		expectedRecordBatch := tester.MakeRandomRecords(numRecords)
		offsets, err := s.AddRecords(expectedRecordBatch)
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, numRecords, offsets)

		// NOTE: in order to prove that we're reading from the backing storage and
		// not from the cache, we're removing the file from the cache.
		err = cacheStorage.Remove(sebtopic.RecordBatchKey(topicName, 0))
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

// TestStorageCompressFiles verifies that Topic uses the given Compress to
// seemlessly compresses and decompresses files when they're written to the
// backing storage.
func TestStorageCompressFiles(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *cache.Cache) {
		const topicName = "topicName"
		compressor := sebtopic.Gzip{}
		s, err := sebtopic.New(log, backingStorage, topicName, cache, sebtopic.WithCompress(compressor))
		require.NoError(t, err)

		const numRecords = 5
		expectedRecordBatch := tester.MakeRandomRecords(numRecords)
		offsets, err := s.AddRecords(expectedRecordBatch)
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, numRecords, offsets)

		backingStorageReader, err := backingStorage.Reader(sebtopic.RecordBatchKey(topicName, 0))
		require.NoError(t, err)

		// read records directly from compressor in order to prove that they're compressed
		compressorReader, err := compressor.NewReader(backingStorageReader)
		require.NoError(t, err)

		buf := tester.ReadToMemory(t, compressorReader)

		parser, err := sebrecords.Parse(buf)
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
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *cache.Cache) {
		s, err := sebtopic.New(log, backingStorage, "topic", cache)
		require.NoError(t, err)

		// no record added yet, next id should be 0
		offset := s.NextOffset()
		require.Equal(t, uint64(0), offset)

		nextOffset := uint64(0)
		for range 10 {
			records := tester.MakeRandomRecords(1 + inty.RandomN(10))
			offsets, err := s.AddRecords(records)
			require.NoError(t, err)
			tester.RequireOffsets(t, nextOffset, nextOffset+uint64(len(records)), offsets)

			nextOffset += uint64(len(records))

			// Act, Assert
			offset := s.NextOffset()
			require.Equal(t, nextOffset, offset)
		}
	})
}

// TestTopicOffsetCond verifies that Topic.OffsetCond.Wait() blocks until the
// expected offset has been reached.
func TestTopicOffsetCond(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *cache.Cache) {
		s, err := sebtopic.New(log, backingStorage, "topic", cache)
		require.NoError(t, err)

		ctx := context.Background()
		returned := make(chan struct{})
		go func() {
			s.OffsetCond.Wait(ctx, 100)
			close(returned)
		}()

		// Wait() is waiting for offset 100, this will add offset 0
		_, err = s.AddRecords(tester.MakeRandomRecords(1))
		require.NoError(t, err)

		time.Sleep(25 * time.Millisecond)
		select {
		case <-returned:
			t.Fatalf("did not expect wait to have returned")
		default:
		}

		// Act
		_, err = s.AddRecords(tester.MakeRandomRecords(100))
		require.NoError(t, err)

		// Assert
		// (test will time out if returned isn't closed)
		<-returned
	})
}

// TestTopicOffsetCondContextExpired verifies that Topic.OffsetCond.Wait()
// returns when the given context expires.
func TestTopicOffsetCondContextExpired(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *cache.Cache) {
		s, err := sebtopic.New(log, backingStorage, "topic", cache)
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

// TestTopicReadRecords verifies that ReadRecords() returns the expected records
// with the given offset, max number of records, and soft max bytes.
func TestTopicReadRecords(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, storage sebtopic.Storage, cache *cache.Cache) {
		topic, err := sebtopic.New(log, storage, "topic", cache)
		require.NoError(t, err)

		const (
			recordSize      = 5
			recordsPerBatch = 10
			batches         = 20
			totalRecords    = recordsPerBatch * batches
		)

		records := []sebrecords.Record{}
		for i := 0; i < batches; i++ {
			expectedRecordBatch := tester.MakeRandomRecordsSize(recordsPerBatch, recordSize)
			_, err := topic.AddRecords(expectedRecordBatch)
			require.NoError(t, err)

			records = append(records, expectedRecordBatch...)
		}

		tests := map[string]struct {
			offset          uint64
			maxRecords      int
			softMaxBytes    int
			expectedRecords []sebrecords.Record
			expectedErr     error
		}{
			"all":                            {offset: 0, maxRecords: totalRecords, expectedRecords: records},
			"offset, first":                  {offset: 0, maxRecords: 1, expectedRecords: records[:1]},
			"offset, last":                   {offset: totalRecords - 1, maxRecords: 1, expectedRecords: records[totalRecords-1:]},
			"offset, mid":                    {offset: totalRecords / 2, maxRecords: 1, expectedRecords: records[totalRecords/2 : totalRecords/2+1]},
			"max records, default":           {offset: 0, maxRecords: 0, expectedRecords: records[:10]},
			"max records, 100":               {offset: 0, maxRecords: totalRecords + 100, expectedRecords: records},
			"max records, 5000":              {offset: 20, maxRecords: 5000, expectedRecords: records[20:]},
			"max records, 20":                {offset: 10, maxRecords: 20, expectedRecords: records[10:30]},
			"max bytes, 20":                  {offset: 10, maxRecords: 50, softMaxBytes: 20 * recordSize, expectedRecords: records[10:30]},
			"max bytes, at least one record": {offset: 10, maxRecords: 50, softMaxBytes: 1, expectedRecords: records[10:11]},
			"max bytes before max records":   {offset: 10, maxRecords: 4, softMaxBytes: 5 * recordSize, expectedRecords: records[10:14]},
		}

		for name, test := range tests {
			t.Run(name, func(t *testing.T) {
				// Act
				gotRecords, err := topic.ReadRecords(context.Background(), test.offset, test.maxRecords, test.softMaxBytes)

				// Assert
				require.NoError(t, err)
				require.Equal(t, test.expectedRecords, gotRecords)
				require.ErrorIs(t, err, test.expectedErr)
			})
		}
	})
}

// TestTopicReadRecordsOutOfBounds verifies that seb.ErrOutOfBounds is returned
// when requesting an offset larger than the existing max offset.
func TestTopicReadRecordsOutOfBounds(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, storage sebtopic.Storage, cache *cache.Cache) {
		topic, err := sebtopic.New(log, storage, "topic", cache)
		require.NoError(t, err)

		expectedRecordBatch := tester.MakeRandomRecords(10)
		_, err = topic.AddRecords(expectedRecordBatch)
		require.NoError(t, err)

		// Act
		_, err = topic.ReadRecords(context.Background(), 100, 1, 0)

		// Assert
		require.ErrorIs(t, err, seb.ErrOutOfBounds)
	})
}

// TestTopicReadRecordsContextExpired verifies that ReadRecords()
// returns the context error when the context expires before returning all of
// the requested records.
func TestTopicReadRecordsContextExpired(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, storage sebtopic.Storage, cache *cache.Cache) {
		topic, err := sebtopic.New(log, storage, "topic", cache)
		require.NoError(t, err)

		expectedRecordBatch := tester.MakeRandomRecords(10)
		_, err = topic.AddRecords(expectedRecordBatch)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // NOTE: canceled immediately

		// Act
		_, err = topic.ReadRecords(ctx, 0, len(expectedRecordBatch), 0)

		// Assert
		require.ErrorIs(t, err, context.Canceled)
	})
}

// TestTopicMetadata verifies that Metadata() returns the most recent metadata.
func TestTopicMetadata(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, storage sebtopic.Storage, cache *cache.Cache) {
		top, err := sebtopic.New(log, storage, "topicName", cache)
		require.NoError(t, err)

		for i := 1; i <= 5; i++ {
			records := tester.MakeRandomRecords(32)
			_, err = top.AddRecords(records)
			require.NoError(t, err)

			// Act
			gotMetadata, err := top.Metadata()
			require.NoError(t, err)
			t0 := time.Now()

			// Assert
			expectedNextOffset := uint64(i * len(records))
			require.Equal(t, expectedNextOffset, gotMetadata.NextOffset)
			require.True(t, timey.DiffEqual(5*time.Millisecond, t0, gotMetadata.LatestCommitAt))
		}
	})
}

// TestTopicMetadataEmptyTopic verifies that Metadata() returns the expected
// data when the topic is empty.
func TestTopicMetadataEmptyTopic(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, storage sebtopic.Storage, cache *cache.Cache) {
		top, err := sebtopic.New(log, storage, "topicName", cache)
		require.NoError(t, err)

		// Act
		gotMetadata, err := top.Metadata()
		require.NoError(t, err)

		// Assert
		expectedMetadata := sebtopic.Metadata{}
		require.Equal(t, expectedMetadata, gotMetadata)
	})
}

// BenchmarkTopicReadBatchUsingReadRecordRepeatedly benchmarks reading a record
// batch using repeated calls to Topic.ReadRecord(). It's here to compare
// against getting the same result doing a single call to Topic.ReadRecords().
func BenchmarkTopicReadBatchUsingReadRecordRepeatedly(b *testing.B) {
	benchmarkTopicReadRecordBatch(b, func(topic *sebtopic.Topic, offset uint64, numRecords int) ([]sebrecords.Record, error) {
		records := make([]sebrecords.Record, numRecords)
		for offset := uint64(0); offset < uint64(numRecords); offset++ {
			record, err := topic.ReadRecord(offset)
			if err != nil {
				return records, err
			}

			records[int(offset)] = record
		}

		return records, nil
	})
}

// BenchmarkTopicReadBatchUsingReadRecords benchmarks reading a record batch
// using Topic.ReadRecords(). It's here to compare against getting the same
// result doing repeated calls to Topic.ReadRecord().
func BenchmarkTopicReadBatchUsingReadRecords(b *testing.B) {
	benchmarkTopicReadRecordBatch(b, func(topic *sebtopic.Topic, offset uint64, numRecords int) ([]sebrecords.Record, error) {
		return topic.ReadRecords(context.Background(), offset, numRecords, 0)
	})
}

func benchmarkTopicReadRecordBatch(b *testing.B, readRecords func(t *sebtopic.Topic, offset uint64, numRecords int) ([]sebrecords.Record, error)) {
	diskCache, err := cache.NewDiskStorage(log, b.TempDir())
	require.NoError(b, err)

	cache, err := cache.New(log, diskCache)
	require.NoError(b, err)

	topicStorage := sebtopic.NewDiskStorage(log, b.TempDir())

	topic, err := sebtopic.New(log, topicStorage, "topic", cache)
	require.NoError(b, err)

	const (
		recordsPerBatch = 10
		batches         = 5
		recordsTotal    = recordsPerBatch * batches
	)

	for i := 0; i < batches; i++ {
		expectedRecordBatch := tester.MakeRandomRecords(recordsPerBatch)
		_, err := topic.AddRecords(expectedRecordBatch)
		require.NoError(b, err)
	}

	b.ResetTimer()
	for range b.N {
		records, err := readRecords(topic, 0, recordsTotal)
		if err != nil {
			b.Fatalf("unexpected error: %s", err)
		}
		if len(records) != recordsTotal {
			b.Fatalf("expected %d records, got %d", recordsTotal, len(records))
		}
	}
}

// BenchmarkTopicReadRecordUsingReadRecord benchmarks reading a single record
// using Topic.ReadRecord(). It's here to compare against doing the same using
// Topic.ReadRecords().
func BenchmarkTopicReadRecordUsingReadRecord(b *testing.B) {
	benchmarkTopicReadRecord(b, func(topic *sebtopic.Topic, offset uint64) (sebrecords.Record, error) {
		return topic.ReadRecord(offset)
	})
}

// BenchmarkTopicReadRecordUsingReadRecords benchmarks reading a single record
// using Topic.ReadRecords(). It's here to compare against doing the same using
// Topic.ReadRecord()
func BenchmarkTopicReadRecordUsingReadRecords(b *testing.B) {
	benchmarkTopicReadRecord(b, func(topic *sebtopic.Topic, offset uint64) (sebrecords.Record, error) {
		records, err := topic.ReadRecords(context.Background(), offset, 1, 0)
		return records[0], err
	})
}

func benchmarkTopicReadRecord(b *testing.B, readRecord func(t *sebtopic.Topic, offset uint64) (sebrecords.Record, error)) {
	diskCache, err := cache.NewDiskStorage(log, b.TempDir())
	require.NoError(b, err)

	cache, err := cache.New(log, diskCache)
	require.NoError(b, err)

	topicStorage := sebtopic.NewDiskStorage(log, b.TempDir())

	topic, err := sebtopic.New(log, topicStorage, "topic", cache)
	require.NoError(b, err)

	expectedRecordBatch := tester.MakeRandomRecords(10)
	topic.AddRecords(expectedRecordBatch)

	const offset = 5

	b.ResetTimer()
	for range b.N {
		record, err := readRecord(topic, offset)
		if err != nil {
			b.Fatalf("unexpected error: %s", err)
		}

		if !slicey.Equal(record, expectedRecordBatch[offset]) {
			b.Fatalf("incorrect record returned: %s vs %s", record, expectedRecordBatch[offset])
		}
	}
}
