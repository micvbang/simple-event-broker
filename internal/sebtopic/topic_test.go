package sebtopic_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/go-helpy/slicey"
	"github.com/micvbang/go-helpy/timey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
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
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *sebcache.Cache) {
		s, err := sebtopic.New(log, backingStorage, "mytopic", cache, sebtopic.WithCompress(nil))
		require.NoError(t, err)

		// Test
		_, err = s.ReadRecords(context.Background(), 0, 0, 0)

		// Verify
		require.ErrorIs(t, err, seb.ErrOutOfBounds)
	})
}

// TestStorageWriteRecordBatchSingleBatch verifies that all records from a
// single Record batch can be read back, and that reading out of bounds returns
// ErrOutOfBounds.
func TestStorageWriteRecordBatchSingleBatch(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *sebcache.Cache) {
		s, err := sebtopic.New(log, backingStorage, "mytopic", cache, sebtopic.WithCompress(nil))
		require.NoError(t, err)

		batch := tester.MakeRandomRecordBatch(5)

		// Test
		offsets, err := s.AddRecords(batch.Sizes(), batch.Data())
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, uint64(batch.Len()), offsets)

		// Verify
		gotBatch, err := s.ReadRecords(context.Background(), offsets[0], batch.Len(), 0)
		require.NoError(t, err)
		require.Equal(t, batch.Data(), gotBatch.Data())

		// Out of bounds reads
		outOfBoundsIndex := uint64(batch.Len())
		_, err = s.ReadRecords(context.Background(), outOfBoundsIndex, 0, 0)
		require.ErrorIs(t, err, seb.ErrOutOfBounds)

		_, err = s.ReadRecords(context.Background(), outOfBoundsIndex+5, 0, 0)
		require.ErrorIs(t, err, seb.ErrOutOfBounds)
	})
}

// TestStorageWriteRecordsBackingStorageWriteFails verifies that an error is
// propagated when a backing storage Writer's Write() fails.
func TestStorageWriteRecordsBackingStorageWriteFails(t *testing.T) {
	tester.TestCacheStorage(t, func(t *testing.T, cacheStorage sebcache.Storage) {
		expectedErr := fmt.Errorf("failed to write to file")

		backingStorage := &tester.MockTopicStorage{}
		backingStorage.ListFilesMock = func(topicName, extension string) ([]sebtopic.File, error) {
			return nil, nil
		}
		backingStorage.WriterMock = func(recordBatchPath string) (io.WriteCloser, error) {
			return &tester.MockWriteCloser{
				WriteMock: func(p []byte) (n int, err error) {
					return 0, expectedErr
				},
			}, nil
		}

		cache, err := sebcache.New(log, cacheStorage)
		require.NoError(t, err)

		s, err := sebtopic.New(log, backingStorage, "mytopic", cache, sebtopic.WithCompress(nil))
		require.NoError(t, err)

		recordSizes, records := tester.MakeRandomRecordsRaw(5)

		// Act
		offsets, err := s.AddRecords(recordSizes, records)

		// Assert
		require.ErrorIs(t, err, expectedErr)
		require.Equal(t, 0, len(offsets))
	})
}

// TestStorageWriteRecordsBackingStorageCloseFails verifies that an error is
// propagated when a backing storage Writer's Close() fails.
func TestStorageWriteRecordsBackingStorageCloseFails(t *testing.T) {
	tester.TestCacheStorage(t, func(t *testing.T, cacheStorage sebcache.Storage) {
		expectedErr := fmt.Errorf("failed to close file")

		backingStorage := &tester.MockTopicStorage{}
		backingStorage.ListFilesMock = func(topicName, extension string) ([]sebtopic.File, error) {
			return nil, nil
		}
		backingStorage.WriterMock = func(recordBatchPath string) (io.WriteCloser, error) {
			return &tester.MockWriteCloser{
				WriteMock: func(p []byte) (n int, err error) {
					return len(p), nil
				},
				CloseMock: func() error {
					return expectedErr
				},
			}, nil
		}

		cache, err := sebcache.New(log, cacheStorage)
		require.NoError(t, err)

		s, err := sebtopic.New(log, backingStorage, "mytopic", cache, sebtopic.WithCompress(nil))
		require.NoError(t, err)

		recordSizes, records := tester.MakeRandomRecordsRaw(5)

		// Act
		offsets, err := s.AddRecords(recordSizes, records)

		// Assert
		require.ErrorIs(t, err, expectedErr)
		require.Equal(t, 0, len(offsets))
	})
}

// TestStorageWriteRecordBatchMultipleBatches verifies that multiple
// RecordBatches can be written to the underlying storage and be read back
// again, and that reading beyond the number of existing records yields
// ErrOutOfBounds.
func TestStorageWriteRecordBatchMultipleBatches(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *sebcache.Cache) {
		s, err := sebtopic.New(log, backingStorage, "mytopic", cache)
		require.NoError(t, err)

		// records1 := tester.MakeRandomRecords(5)
		// recordSizes1, rawRecords1 := tester.RecordsConcat(records1)
		batch1 := tester.MakeRandomRecordBatch(5)
		batch2 := tester.MakeRandomRecordBatch(3)
		// records2 := tester.MakeRandomRecords(3)
		// recordSizes2, rawRecords2 := tester.RecordsConcat(records2)

		// Test
		b1Offsets, err := s.AddRecords(batch1.Sizes(), batch1.Data())
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, 5, b1Offsets)

		b2Offsets, err := s.AddRecords(batch2.Sizes(), batch2.Data())
		require.NoError(t, err)
		tester.RequireOffsets(t, 5, 8, b2Offsets)

		// Verify
		expectedRecords := append(batch1.Data(), batch2.Data()...)
		gotBatch, err := s.ReadRecords(context.Background(), b1Offsets[0], len(expectedRecords), 0)
		require.NoError(t, err)
		require.Equal(t, expectedRecords, gotBatch.Data())

		// Out of bounds reads
		_, err = s.ReadRecords(context.Background(), uint64(batch1.Len()+batch2.Len()), 0, 0)
		require.ErrorIs(t, err, seb.ErrOutOfBounds)
	})
}

// TestStorageOpenExistingStorage verifies that storage.Storage correctly
// initializes from a topic that already exists and has many data files.
func TestStorageOpenExistingStorage(t *testing.T) {
	tester.TestBackingStorage(t, func(t *testing.T, backingStorage sebtopic.Storage) {
		const topicName = "my_topic"

		totalRecords := 0
		batches := make([]sebrecords.Batch, 50)
		for i := 0; i < len(batches); i++ {
			batchSize := 1 + inty.RandomN(5)
			totalRecords += batchSize
			batches[i] = tester.MakeRandomRecordBatch(batchSize)
		}

		{
			cache, err := sebcache.New(log, sebcache.NewMemoryStorage(log))
			require.NoError(t, err)
			s1, err := sebtopic.New(log, backingStorage, topicName, cache)
			require.NoError(t, err)

			batchStartID := uint64(0)
			for _, batch := range batches {
				batchEndID := batchStartID + uint64(batch.Len())

				offsets, err := s1.AddRecords(batch.Sizes(), batch.Data())
				require.NoError(t, err)
				tester.RequireOffsets(t, batchStartID, batchEndID, offsets)

				batchStartID += uint64(batch.Len())
			}
		}

		cache, err := sebcache.New(log, sebcache.NewMemoryStorage(log))
		require.NoError(t, err)

		// Test
		s2, err := sebtopic.New(log, backingStorage, topicName, cache)
		require.NoError(t, err)

		// Verify
		offset := uint64(0)
		for _, batch := range batches {
			gotBatch, err := s2.ReadRecords(context.Background(), offset, batch.Len(), 0)
			require.NoError(t, err)
			require.Equal(t, batch, gotBatch)
			offset += uint64(batch.Len())
		}

		// Out of bounds reads
		_, err = s2.ReadRecords(context.Background(), uint64(totalRecords+1), 0, 0)
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

		batch1 := tester.MakeRandomRecordBatch(1)
		{
			cache, err := sebcache.New(log, sebcache.NewMemoryStorage(log))
			require.NoError(t, err)
			s1, err := sebtopic.New(log, topicStorage, topicName, cache)
			require.NoError(t, err)

			// recordSizes, rawRecords := tester.RecordsConcat(batch1)
			offsets, err := s1.AddRecords(batch1.Sizes(), batch1.Data())
			require.NoError(t, err)
			tester.RequireOffsets(t, 0, 1, offsets)
		}

		cache, err := sebcache.New(log, sebcache.NewMemoryStorage(log))
		require.NoError(t, err)

		s2, err := sebtopic.New(log, topicStorage, topicName, cache)
		require.NoError(t, err)

		// Test
		batch2 := tester.MakeRandomRecordBatch(1)
		offsets, err := s2.AddRecords(batch2.Sizes(), batch2.Data())
		require.NoError(t, err)
		tester.RequireOffsets(t, 1, 2, offsets)

		// Verify
		expectedRecords := append(batch1.Data(), batch2.Data()...)
		gotBatch, err := s2.ReadRecords(context.Background(), 0, 0, 0)
		require.NoError(t, err)
		require.Equal(t, expectedRecords, gotBatch.Data())

		// Out of bounds reads
		_, err = s2.ReadRecords(context.Background(), uint64(len(expectedRecords)), 0, 0)
		require.ErrorIs(t, err, seb.ErrOutOfBounds)
	})
}

// TestStorageCacheWrite verifies that AddRecordBatch uses the cache to cache
// the record batch.
func TestStorageCacheWrite(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *sebcache.Cache) {
		const topicName = "my_topic"

		s, err := sebtopic.New(log, backingStorage, topicName, cache)
		require.NoError(t, err)

		batchKey := sebtopic.RecordBatchKey(topicName, 0)
		const numRecords = 5
		batch := tester.MakeRandomRecordBatch(numRecords)

		// Act
		offsets, err := s.AddRecords(batch.Sizes(), batch.Data())
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, numRecords, offsets)

		// Assert

		// record batch must be written to both backing storage and cache.
		_, err = cache.Reader(batchKey)
		require.NoError(t, err)

		_, err = backingStorage.Reader(batchKey)
		require.NoError(t, err)

		gotBatch, err := s.ReadRecords(context.Background(), offsets[0], 0, 0)
		require.NoError(t, err)
		require.Equal(t, batch.Data(), gotBatch.Data())
	})
}

// TestStorageCacheWrite verifies that ReadRecord uses the cache to read
// results.
func TestStorageCacheReadFromCache(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *sebcache.Cache) {
		const topicName = "my_topic"

		s, err := sebtopic.New(log, backingStorage, topicName, cache)
		require.NoError(t, err)

		const numRecords = 5
		batch := tester.MakeRandomRecordBatch(numRecords)
		// recordSizes, rawRecords := tester.RecordsConcat(expectedRecordBatch)
		offsets, err := s.AddRecords(batch.Sizes(), batch.Data())
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, numRecords, offsets)

		// NOTE: in order to prove that we're reading from the cache and not
		// from the backing storage, we're truncating the file in the backing
		// storage to zero bytes.
		wtr, err := backingStorage.Writer(sebtopic.RecordBatchKey(topicName, 0))
		require.NoError(t, err)
		tester.WriteAndClose(t, wtr, []byte{})

		gotBatch, err := s.ReadRecords(context.Background(), offsets[0], 0, 0)
		require.NoError(t, err)
		require.Equal(t, batch, gotBatch)
	})
}

// TestStorageCacheReadFileNotInCache verifies that ReadRecord can fetch record
// batches from the backing storage if it's not in the cache.
func TestStorageCacheReadFileNotInCache(t *testing.T) {
	tester.TestBackingStorage(t, func(t *testing.T, backingStorage sebtopic.Storage) {
		const topicName = "my_topic"

		cacheStorage := sebcache.NewMemoryStorage(log)
		cache, err := sebcache.New(log, cacheStorage)
		require.NoError(t, err)

		s, err := sebtopic.New(log, backingStorage, topicName, cache)
		require.NoError(t, err)

		const numRecords = 5
		batch := tester.MakeRandomRecordBatch(numRecords)
		offsets, err := s.AddRecords(batch.Sizes(), batch.Data())
		require.NoError(t, err)
		tester.RequireOffsets(t, 0, numRecords, offsets)

		// NOTE: in order to prove that we're reading from the backing storage and
		// not from the cache, we're removing the file from the cache.
		err = cacheStorage.Remove(sebtopic.RecordBatchKey(topicName, 0))
		require.NoError(t, err)

		gotBatch, err := s.ReadRecords(context.Background(), offsets[0], 0, 0)
		require.NoError(t, err)
		require.Equal(t, batch.Data(), gotBatch.Data())
	})
}

// TestStorageCompressFiles verifies that Topic uses the given Compress to
// seemlessly compresses and decompresses files when they're written to the
// backing storage.
func TestStorageCompressFiles(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *sebcache.Cache) {
		const topicName = "topicName"
		compressor := sebtopic.Gzip{}
		s, err := sebtopic.New(log, backingStorage, topicName, cache, sebtopic.WithCompress(compressor))
		require.NoError(t, err)

		const numRecords = 5
		batch := tester.MakeRandomRecordBatch(numRecords)
		offsets, err := s.AddRecords(batch.Sizes(), batch.Data())
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
		require.Equal(t, uint32(batch.Len()), parser.Header.NumRecords)

		// can read records from compressed data
		gotBatch, err := s.ReadRecords(context.Background(), offsets[0], 0, 0)
		require.NoError(t, err)
		require.Equal(t, batch.Data(), gotBatch.Data())
	})
}

// TestTopicEndOffset verifies that EndOffset returns the offset of the
// next record that is added, i.e. the id of most-recently-added+1.
func TestTopicEndOffset(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *sebcache.Cache) {
		s, err := sebtopic.New(log, backingStorage, "topic", cache)
		require.NoError(t, err)

		// no record added yet, next id should be 0
		offset := s.NextOffset()
		require.Equal(t, uint64(0), offset)

		nextOffset := uint64(0)
		for range 10 {
			recordSizes, rawRecords := tester.MakeRandomRecordsRaw(1 + inty.RandomN(10))
			offsets, err := s.AddRecords(recordSizes, rawRecords)
			require.NoError(t, err)
			tester.RequireOffsets(t, nextOffset, nextOffset+uint64(len(recordSizes)), offsets)

			nextOffset += uint64(len(recordSizes))

			// Act, Assert
			offset := s.NextOffset()
			require.Equal(t, nextOffset, offset)
		}
	})
}

// TestTopicOffsetCond verifies that Topic.OffsetCond.Wait() blocks until the
// expected offset has been reached.
func TestTopicOffsetCond(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *sebcache.Cache) {
		s, err := sebtopic.New(log, backingStorage, "topic", cache)
		require.NoError(t, err)

		ctx := context.Background()
		returned := make(chan struct{})
		go func() {
			s.OffsetCond.Wait(ctx, 100)
			close(returned)
		}()

		// Wait() is waiting for offset 100, this will add offset 0
		recordSizes, rawRecords := tester.MakeRandomRecordsRaw(1)
		_, err = s.AddRecords(recordSizes, rawRecords)
		require.NoError(t, err)

		time.Sleep(25 * time.Millisecond)
		select {
		case <-returned:
			t.Fatalf("did not expect wait to have returned")
		default:
		}

		// Act
		recordSizes, rawRecords = tester.MakeRandomRecordsRaw(100)
		_, err = s.AddRecords(recordSizes, rawRecords)
		require.NoError(t, err)

		// Assert
		// (test will time out if returned isn't closed)
		<-returned
	})
}

// TestTopicOffsetCondContextExpired verifies that Topic.OffsetCond.Wait()
// returns when the given context expires.
func TestTopicOffsetCondContextExpired(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, backingStorage sebtopic.Storage, cache *sebcache.Cache) {
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
	tester.TestTopicStorageAndCache(t, func(t *testing.T, storage sebtopic.Storage, cache *sebcache.Cache) {
		topic, err := sebtopic.New(log, storage, "topic", cache)
		require.NoError(t, err)

		const (
			recordSize      = 5
			recordsPerBatch = 10
			batches         = 20
			totalRecords    = recordsPerBatch * batches
		)

		records := [][]byte{}
		for i := 0; i < batches; i++ {
			batch := tester.MakeRandomRecordBatchSize(recordsPerBatch, recordSize)
			_, err := topic.AddRecords(batch.Sizes(), batch.Data())
			require.NoError(t, err)

			records = append(records, tester.BatchIndividualRecords(t, batch, 0, batch.Len())...)
		}

		tests := map[string]struct {
			offset          uint64
			maxRecords      int
			softMaxBytes    int
			expectedRecords [][]byte
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
			"max records, offset into middle of batch":      {offset: 13, maxRecords: 13, expectedRecords: records[13:26]},
			"max bytes, offset into middle of batch":        {offset: 13, maxRecords: totalRecords, softMaxBytes: recordSize * 13, expectedRecords: records[13:26]},
			"last record of batch, not first of next batch": {offset: 9, maxRecords: 10, softMaxBytes: recordSize + 1, expectedRecords: records[9:10]},
		}

		for name, test := range tests {
			t.Run(name, func(t *testing.T) {
				// Act
				gotBatch, err := topic.ReadRecords(context.Background(), test.offset, test.maxRecords, test.softMaxBytes)

				// Assert
				require.NoError(t, err)
				require.Equal(t, test.expectedRecords, tester.BatchIndividualRecords(t, gotBatch, 0, gotBatch.Len()))
				require.ErrorIs(t, err, test.expectedErr)
			})
		}
	})
}

// TestTopicReadRecordsRandomRecordSizes verifies that ReadRecords() returns the
// expected records with the given offset, max number of records, and soft max
// bytes, when the input record batches are randomly sized.
func TestTopicReadRecordsRandomRecordSizes(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, storage sebtopic.Storage, cache *sebcache.Cache) {
		topic, err := sebtopic.New(log, storage, "topic", cache)
		require.NoError(t, err)

		const (
			recordsPerBatch = 10
			batches         = 20
			totalRecords    = recordsPerBatch * batches
		)

		records := [][]byte{}
		recordSizes := []int{}
		for i := 0; i < batches; i++ {
			batch := tester.MakeRandomRecordBatch(recordsPerBatch)
			_, err := topic.AddRecords(batch.Sizes(), batch.Data())
			require.NoError(t, err)

			records = append(records, tester.BatchIndividualRecords(t, batch, 0, batch.Len())...)
		}

		for _, record := range records {
			recordSizes = append(recordSizes, len(record))
		}

		tests := map[string]struct {
			offset          uint64
			maxRecords      int
			softMaxBytes    int
			expectedRecords [][]byte
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
			"max bytes, 20":                  {offset: 10, maxRecords: 50, softMaxBytes: slicey.Sum(recordSizes[10:30]), expectedRecords: records[10:30]},
			"max bytes, at least one record": {offset: 10, maxRecords: 50, softMaxBytes: 1, expectedRecords: records[10:11]},
			"max bytes before max records":   {offset: 10, maxRecords: 4, softMaxBytes: slicey.Sum(recordSizes[10:14]), expectedRecords: records[10:14]},
			"max records, offset into middle of batch": {offset: 13, maxRecords: totalRecords, expectedRecords: records[13:]},
			"max bytes, offset into middle of batch":   {offset: 13, maxRecords: totalRecords, softMaxBytes: slicey.Sum(recordSizes[13:30]), expectedRecords: records[13:30]},
		}

		for name, test := range tests {
			t.Run(name, func(t *testing.T) {
				// Act
				gotBatch, err := topic.ReadRecords(context.Background(), test.offset, test.maxRecords, test.softMaxBytes)

				// Assert
				require.NoError(t, err)
				require.Equal(t, test.expectedRecords, tester.BatchIndividualRecords(t, gotBatch, 0, gotBatch.Len()))
				require.ErrorIs(t, err, test.expectedErr)
			})
		}
	})
}

// TestTopicReadRecordsOutOfBounds verifies that seb.ErrOutOfBounds is returned
// when requesting an offset larger than the existing max offset.
func TestTopicReadRecordsOutOfBounds(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, storage sebtopic.Storage, cache *sebcache.Cache) {
		topic, err := sebtopic.New(log, storage, "topic", cache)
		require.NoError(t, err)

		recordSizes, rawRecords := tester.MakeRandomRecordsRaw(10)
		_, err = topic.AddRecords(recordSizes, rawRecords)
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
	tester.TestTopicStorageAndCache(t, func(t *testing.T, storage sebtopic.Storage, cache *sebcache.Cache) {
		topic, err := sebtopic.New(log, storage, "topic", cache)
		require.NoError(t, err)

		recordSizes, rawRecords := tester.MakeRandomRecordsRaw(10)
		_, err = topic.AddRecords(recordSizes, rawRecords)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // NOTE: canceled immediately

		// Act
		_, err = topic.ReadRecords(ctx, 0, len(recordSizes), 0)

		// Assert
		require.ErrorIs(t, err, context.Canceled)
	})
}

// TestTopicMetadata verifies that Metadata() returns the most recent metadata.
func TestTopicMetadata(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, storage sebtopic.Storage, cache *sebcache.Cache) {
		topic, err := sebtopic.New(log, storage, "topicName", cache)
		require.NoError(t, err)

		for i := 1; i <= 5; i++ {
			recordSizes, rawRecords := tester.MakeRandomRecordsRaw(32)
			_, err = topic.AddRecords(recordSizes, rawRecords)
			require.NoError(t, err)

			// Act
			gotMetadata, err := topic.Metadata()
			require.NoError(t, err)
			t0 := time.Now()

			// Assert
			expectedNextOffset := uint64(i * len(recordSizes))
			require.Equal(t, expectedNextOffset, gotMetadata.NextOffset)
			require.True(t, timey.DiffEqual(5*time.Millisecond, t0, gotMetadata.LatestCommitAt))
		}
	})
}

// TestTopicMetadataEmptyTopic verifies that Metadata() returns the expected
// data when the topic is empty.
func TestTopicMetadataEmptyTopic(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, storage sebtopic.Storage, cache *sebcache.Cache) {
		topic, err := sebtopic.New(log, storage, "topicName", cache)
		require.NoError(t, err)

		// Act
		gotMetadata, err := topic.Metadata()
		require.NoError(t, err)

		// Assert
		expectedMetadata := sebtopic.Metadata{}
		require.Equal(t, expectedMetadata, gotMetadata)
	})
}

// BenchmarkTopicReadBatchUsingReadRecords benchmarks reading a record batch
// using Topic.ReadRecords().
func BenchmarkTopicReadBatchUsingReadRecords(b *testing.B) {
	benchmarkTopicReadRecordBatch(b, func(topic *sebtopic.Topic, offset uint64, numRecords int) (sebrecords.Batch, error) {
		return topic.ReadRecords(context.Background(), offset, numRecords, 0)
	})
}

func benchmarkTopicReadRecordBatch(b *testing.B, readRecords func(t *sebtopic.Topic, offset uint64, numRecords int) (sebrecords.Batch, error)) {
	diskCache, err := sebcache.NewDiskStorage(log, b.TempDir())
	require.NoError(b, err)

	cache, err := sebcache.New(log, diskCache)
	require.NoError(b, err)

	topicStorage := sebtopic.NewDiskStorage(log, b.TempDir())

	topic, err := sebtopic.New(log, topicStorage, "topic", cache)
	require.NoError(b, err)

	const (
		recordsPerBatch = 100
		batches         = 50
		recordsTotal    = recordsPerBatch * batches
	)

	for i := 0; i < batches; i++ {
		recordSizes, rawRecords := tester.MakeRandomRecordsRaw(recordsPerBatch)
		_, err := topic.AddRecords(recordSizes, rawRecords)
		require.NoError(b, err)
	}

	b.ResetTimer()
	for range b.N {
		batch, err := readRecords(topic, 0, recordsTotal)
		if err != nil {
			b.Fatalf("unexpected error: %s", err)
		}
		if batch.Len() != recordsTotal {
			b.Fatalf("expected %d records, got %d", recordsTotal, batch.Len())
		}
	}
}

// BenchmarkTopicReadRecordUsingReadRecords benchmarks reading a single record
// using Topic.ReadRecords().
func BenchmarkTopicReadRecordUsingReadRecords(b *testing.B) {
	benchmarkTopicReadRecords(b, func(topic *sebtopic.Topic, offset uint64) ([]byte, error) {
		batch, err := topic.ReadRecords(context.Background(), offset, 1, 0)
		if err != nil {
			return nil, err
		}
		return batch.Records(0, 1)
	})
}

func benchmarkTopicReadRecords(b *testing.B, readRecord func(t *sebtopic.Topic, offset uint64) ([]byte, error)) {
	diskCache, err := sebcache.NewDiskStorage(log, b.TempDir())
	require.NoError(b, err)

	cache, err := sebcache.New(log, diskCache)
	require.NoError(b, err)

	topicStorage := sebtopic.NewDiskStorage(log, b.TempDir())

	topic, err := sebtopic.New(log, topicStorage, "topic", cache)
	require.NoError(b, err)

	// expectedRecordBatch := tester.MakeRandomRecords(10)
	// recordSizes, records := tester.RecordsConcat(expectedRecordBatch)
	batch := tester.MakeRandomRecordBatch(10)
	topic.AddRecords(batch.Sizes(), batch.Data())

	const offset = 5

	b.ResetTimer()
	for range b.N {
		record, err := readRecord(topic, offset)
		if err != nil {
			b.Fatalf("unexpected error: %s", err)
		}

		gotRecord := tester.BatchIndividualRecords(b, batch, offset, offset+1)[0]
		if !slicey.Equal(record, gotRecord) {
			b.Fatalf("incorrect record returned: %s vs %s", record, gotRecord)
		}
	}
}
