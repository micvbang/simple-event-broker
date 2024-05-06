package storage_test

import (
	"context"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/micvbang/simple-event-broker/internal/tester"
	"github.com/stretchr/testify/require"
)

// TestGetRecordsOffsetAndMaxCount verifies that the expected records are
// returned when requesting different offsets, max records, and soft max byte
// limits.
func TestGetRecordsOffsetAndMaxCount(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, bs storage.BackingStorage, cache *storage.Cache) {
		const topicName = "topic-name"
		ctx := context.Background()

		s := storage.New(log,
			func(log logger.Logger, topicName string) (*storage.Topic, error) {
				return storage.NewTopic(log, bs, topicName, cache, &storage.Gzip{})
			},
			func(l logger.Logger, t *storage.Topic) storage.RecordBatcher {
				return storage.NewNullBatcher(t.AddRecordBatch)
			},
		)

		const recordSize = 16

		allRecords := make(recordbatch.RecordBatch, 32)
		for i := range len(allRecords) {
			allRecords[i] = tester.RandomBytes(t, recordSize)

			_, err := s.AddRecord(topicName, allRecords[i])
			require.NoError(t, err)
		}

		tests := map[string]struct {
			offset       uint64
			maxRecords   int
			softMaxBytes int
			expected     recordbatch.RecordBatch
		}{
			"max records zero":    {offset: 0, maxRecords: 0, expected: allRecords[:0]},
			"0-1":                 {offset: 0, maxRecords: 1, expected: allRecords[:1]},
			"0-4":                 {offset: 0, maxRecords: 5, expected: allRecords[0:5]},
			"1-5":                 {offset: 1, maxRecords: 5, expected: allRecords[1:6]},
			"6-6":                 {offset: 6, maxRecords: 1, expected: allRecords[6:7]},
			"0-100":               {offset: 0, maxRecords: 100, expected: allRecords[:]},
			"32-100":              {offset: 32, maxRecords: 100, expected: allRecords[:0]},
			"soft max 5 records":  {offset: 3, maxRecords: 10, softMaxBytes: recordSize * 5, expected: allRecords[3:8]},
			"soft max 10 records": {offset: 7, maxRecords: 10, softMaxBytes: recordSize * 10, expected: allRecords[7:17]},
			"max records 10":      {offset: 5, maxRecords: 10, softMaxBytes: recordSize * 15, expected: allRecords[5:15]},

			// softMaxBytes is only a soft max; return at least one record, even
			// if that record is larger than the soft max.
			"soft max one byte": {offset: 5, maxRecords: 10, softMaxBytes: 1, expected: allRecords[5:6]},
		}

		for name, test := range tests {
			t.Run(name, func(t *testing.T) {
				// Act
				got, err := s.GetRecords(ctx, topicName, test.offset, test.maxRecords, test.softMaxBytes)
				require.NoError(t, err)

				// Assert
				require.Equal(t, len(test.expected), len(got))
				require.Equal(t, test.expected, got)
			})
		}
	})
}

// TestStorageAutoCreateTopic verifies that AddRecords returns
// storage.ErrTopicNotFound when autoCreateTopic is false, and automatically
// creates the topic if it is true.
func TestAddRecordAutoCreateTopic(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, bs storage.BackingStorage, cache *storage.Cache) {
		const topicName = "topic-name"
		expectedRecord := recordbatch.Record("this is a record")

		tests := map[string]struct {
			autoCreateTopic bool
			err             error
		}{
			"false": {autoCreateTopic: false, err: storage.ErrTopicNotFound},
			"true":  {autoCreateTopic: true, err: nil},
		}

		for name, test := range tests {
			t.Run(name, func(t *testing.T) {
				s := storage.NewWithAutoCreate(log,
					func(log logger.Logger, topicName string) (*storage.Topic, error) {
						return storage.NewTopic(log, bs, topicName, cache, &storage.Gzip{})
					},
					func(l logger.Logger, t *storage.Topic) storage.RecordBatcher {
						return storage.NewNullBatcher(t.AddRecordBatch)
					},
					test.autoCreateTopic,
				)

				// Act
				_, err := s.AddRecord(topicName, expectedRecord)

				// Assert
				require.ErrorIs(t, err, test.err)
			})
		}
	})
}

// TestGetRecordsTopicDoesNotExist verifies that GetRecords returns an empty
// record batch when attempting to read from a topic that does not exist.
func TestGetRecordsTopicDoesNotExist(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, bs storage.BackingStorage, cache *storage.Cache) {
		const topicName = "topic-name"
		ctx := context.Background()
		record := tester.RandomBytes(t, 8)

		tests := map[string]struct {
			autoCreateTopic bool
			err             error
		}{
			"false": {autoCreateTopic: false, err: storage.ErrTopicNotFound},
			"true":  {autoCreateTopic: true, err: nil},
		}

		for name, test := range tests {
			t.Run(name, func(t *testing.T) {
				s := storage.NewWithAutoCreate(log,
					func(log logger.Logger, topicName string) (*storage.Topic, error) {
						return storage.NewTopic(log, bs, topicName, cache, &storage.Gzip{})
					},
					func(l logger.Logger, t *storage.Topic) storage.RecordBatcher {
						return storage.NewNullBatcher(t.AddRecordBatch)
					},
					test.autoCreateTopic,
				)

				// will return an error if autoCreateTopic is false
				_, err := s.AddRecord(topicName, record)
				require.ErrorIs(t, err, test.err)

				// Act
				got, err := s.GetRecords(ctx, "does-not-exist", 0, 10, 1024)
				require.ErrorIs(t, err, test.err)

				// Assert
				require.Equal(t, recordbatch.RecordBatch{}, got)
			})
		}
	})
}

// TestGetRecordsBulkContextImmediatelyCancelled verifies that GetRecords
// respects that the given context has been called.
func TestGetRecordsBulkContextImmediatelyCancelled(t *testing.T) {
	tester.TestBackingStorageAndCache(t, func(t *testing.T, bs storage.BackingStorage, cache *storage.Cache) {
		const topicName = "topic-name"

		s := storage.New(log,
			func(log logger.Logger, topicName string) (*storage.Topic, error) {
				return storage.NewTopic(log, bs, topicName, cache, &storage.Gzip{})
			},
			func(l logger.Logger, t *storage.Topic) storage.RecordBatcher {
				return storage.NewNullBatcher(t.AddRecordBatch)
			},
		)

		allRecords := tester.MakeRandomRecordBatch(5)
		for _, record := range allRecords {
			_, err := s.AddRecord(topicName, record)
			require.NoError(t, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Act
		got, err := s.GetRecords(ctx, topicName, 0, 10, 1024)

		// Assert
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, recordbatch.RecordBatch{}, got)
	})
}
