package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/micvbang/go-helpy/timey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/cache"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
	"github.com/micvbang/simple-event-broker/internal/storage"
	"github.com/micvbang/simple-event-broker/internal/topic"
	"github.com/stretchr/testify/require"
)

var (
	log = logger.NewWithLevel(context.Background(), logger.LevelWarn)
)

// TestGetRecordsOffsetAndMaxCount verifies that the expected records are
// returned when requesting different offsets, max records, and soft max byte
// limits.
func TestGetRecordsOffsetAndMaxCount(t *testing.T) {
	const autoCreateTopic = true
	tester.TestStorage(t, autoCreateTopic, func(t *testing.T, s *storage.Storage) {
		const topicName = "topic-name"
		ctx := context.Background()

		const (
			recordSize        = 16
			maxRecordsDefault = 10
		)

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
			err          error
		}{
			"max records zero":          {offset: 0, maxRecords: 0, expected: allRecords[:maxRecordsDefault]},
			"0-1":                       {offset: 0, maxRecords: 1, expected: allRecords[:1]},
			"0-4":                       {offset: 0, maxRecords: 5, expected: allRecords[0:5]},
			"1-5":                       {offset: 1, maxRecords: 5, expected: allRecords[1:6]},
			"6-6":                       {offset: 6, maxRecords: 1, expected: allRecords[6:7]},
			"0-100":                     {offset: 0, maxRecords: 100, expected: allRecords},
			"32-100 (out of bounds)":    {offset: 32, maxRecords: 100, expected: nil, err: context.DeadlineExceeded},
			"soft max bytes 5 records":  {offset: 3, maxRecords: 10, softMaxBytes: recordSize * 5, expected: allRecords[3:8]},
			"soft max bytes 10 records": {offset: 7, maxRecords: 10, softMaxBytes: recordSize * 10, expected: allRecords[7:17]},
			"max records 10":            {offset: 5, maxRecords: 10, softMaxBytes: recordSize * 15, expected: allRecords[5:15]},

			// softMaxBytes is only a soft max; return at least one record, even
			// if that record is larger than the soft max.
			"soft max one byte": {offset: 5, maxRecords: 10, softMaxBytes: 1, expected: allRecords[5:6]},
		}

		for name, test := range tests {
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()

				// Act
				got, err := s.GetRecords(ctx, topicName, test.offset, test.maxRecords, test.softMaxBytes)
				require.ErrorIs(t, err, test.err)

				// Assert
				require.Equal(t, len(test.expected), len(got))
				require.Equal(t, test.expected, got)
			})
		}
	})
}

// TestAddRecordsAutoCreateTopic verifies that AddRecord and AddRecords returns
// seb.ErrTopicNotFound when autoCreateTopic is false, and automatically creates
// the topic when it is true.
func TestAddRecordsAutoCreateTopic(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, ts topic.Storage, cache *cache.Cache) {
		tests := map[string]struct {
			autoCreateTopic bool
			err             error
		}{
			"false": {autoCreateTopic: false, err: seb.ErrTopicNotFound},
			"true":  {autoCreateTopic: true, err: nil},
		}

		for name, test := range tests {
			t.Run(name, func(t *testing.T) {
				s := storage.New(log,
					storage.NewTopicFactory(ts, cache),
					storage.WithNullBatcher(),
					storage.WithAutoCreateTopic(test.autoCreateTopic),
				)

				// AddRecord
				{
					// Act
					_, err := s.AddRecord("first", recordbatch.Record("this is a record"))

					// Assert
					require.ErrorIs(t, err, test.err)
				}

				// AddRecords
				{
					// Act
					_, err := s.AddRecords("second", tester.MakeRandomRecordBatch(5))

					// Assert
					require.ErrorIs(t, err, test.err)
				}
			})
		}
	})
}

// TestGetRecordsTopicDoesNotExist verifies that GetRecords returns an empty
// record batch when attempting to read from a topic that does not exist.
func TestGetRecordsTopicDoesNotExist(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, ts topic.Storage, cache *cache.Cache) {
		const topicName = "topic-name"
		ctx := context.Background()
		record := tester.RandomBytes(t, 8)

		tests := map[string]struct {
			autoCreateTopic bool
			addErr          error
			getErr          error
		}{
			"false": {autoCreateTopic: false, addErr: seb.ErrTopicNotFound, getErr: seb.ErrTopicNotFound},
			"true":  {autoCreateTopic: true, addErr: nil, getErr: seb.ErrOutOfBounds},
		}

		for name, test := range tests {
			t.Run(name, func(t *testing.T) {
				s := storage.New(log,
					storage.NewTopicFactory(ts, cache),
					storage.WithNullBatcher(),
					storage.WithAutoCreateTopic(test.autoCreateTopic),
				)

				// will return an error if autoCreateTopic is false
				_, err := s.AddRecord(topicName, record)
				require.ErrorIs(t, err, test.addErr)

				// Act
				got, err := s.GetRecords(ctx, "does-not-exist", 0, 10, 1024)
				require.ErrorIs(t, err, test.getErr)

				// Assert
				var expected recordbatch.RecordBatch
				require.Equal(t, expected, got)
			})
		}
	})
}

// TestGetRecordsOffsetOutOfBounds verifies that GetRecords returns
// context.DeadlineExceeded when attempting to read an offset that is too high
// (does not yet exist).
func TestGetRecordsOffsetOutOfBounds(t *testing.T) {
	const autoCreateTopic = true
	tester.TestStorage(t, autoCreateTopic, func(t *testing.T, s *storage.Storage) {
		const topicName = "topic-name"

		// add record so that we know there's _something_ in the topic
		offset, err := s.AddRecord(topicName, tester.RandomBytes(t, 8))
		require.NoError(t, err)

		nonExistingOffset := offset + 5

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		// Act
		_, err = s.GetRecords(ctx, "does-not-exist", nonExistingOffset, 10, 1024)

		// Assert
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

// TestGetRecordsBulkContextImmediatelyCancelled verifies that GetRecords
// respects that the given context has been called.
func TestGetRecordsBulkContextImmediatelyCancelled(t *testing.T) {
	autoCreateTopic := true
	tester.TestStorage(t, autoCreateTopic, func(t *testing.T, s *storage.Storage) {
		const topicName = "topic-name"

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

// TestCreateTopicHappyPath verifies that CreateTopic creates a topic, and that
// GetRecord() and AddRecord() are only successful once the topic has been
// created.
func TestCreateTopicHappyPath(t *testing.T) {
	const autoCreateTopic = false
	tester.TestStorage(t, autoCreateTopic, func(t *testing.T, s *storage.Storage) {
		const topicName = "topic-name"

		_, err := s.GetRecord(topicName, 0)
		require.ErrorIs(t, err, seb.ErrTopicNotFound)

		_, err = s.AddRecord(topicName, recordbatch.Record("this is a record"))
		require.ErrorIs(t, err, seb.ErrTopicNotFound)

		// Act
		err = s.CreateTopic(topicName)
		require.NoError(t, err)

		// Assert
		_, err = s.GetRecord(topicName, 0)
		require.ErrorIs(t, err, seb.ErrOutOfBounds)

		_, err = s.AddRecord(topicName, recordbatch.Record("this is a record"))
		require.NoError(t, err)
	})
}

// TestCreateTopicAlreadyExistsInStorage verifies that calling CreateTopic on
// different instances of storage.Storage returns ErrTopicAlreadyExists when
// attempting to create a topic that already exists in topic storage (at least
// one record was added to the topic in its lifetime)
func TestCreateTopicAlreadyExistsInStorage(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, bs topic.Storage, cache *cache.Cache) {
		const topicName = "topic-name"

		{
			s1 := storage.New(log,
				func(log logger.Logger, topicName string) (*topic.Topic, error) {
					return topic.New(log, bs, topicName, cache)
				},
				storage.WithNullBatcher(),
				storage.WithAutoCreateTopic(false),
			)

			err := s1.CreateTopic(topicName)
			require.NoError(t, err)

			// NOTE: the test relies on there being a created at least one
			// record in topic storage, since that's the only (current) way to
			// persist information about a topic's existence.
			_, err = s1.AddRecord(topicName, recordbatch.Record("this is a record"))
			require.NoError(t, err)
		}

		{
			s2 := storage.New(log,
				func(log logger.Logger, topicName string) (*topic.Topic, error) {
					return topic.New(log, bs, topicName, cache)
				},
				storage.WithNullBatcher(),
				storage.WithAutoCreateTopic(false),
			)

			// Act
			err := s2.CreateTopic(topicName)

			// Assert
			// we expect Storage to complain that topic alreay exists, because
			// it exists in the backing storage.
			require.ErrorIs(t, err, seb.ErrTopicAlreadyExists)
		}
	})
}

// TestCreateTopicAlreadyExists verifies that calling CreateTopic on the same
// instance of storage.Storage will return ErrTopicAlreadyExists if the topic
// was already created.
func TestCreateTopicAlreadyExists(t *testing.T) {
	const autoCreateTopic = false
	tester.TestStorage(t, autoCreateTopic, func(t *testing.T, s *storage.Storage) {
		const topicName = "topic-name"

		// Act
		err := s.CreateTopic(topicName)
		require.NoError(t, err)

		// Assert
		err = s.CreateTopic(topicName)
		require.ErrorIs(t, err, seb.ErrTopicAlreadyExists)
	})
}

// TestStorageMetadataHappyPath verifies that Metadata() returns the expected
// data for a topic that exists.
func TestStorageMetadataHappyPath(t *testing.T) {
	const autoCreate = true
	tester.TestStorage(t, autoCreate, func(t *testing.T, s *storage.Storage) {
		const topicName = "topic-name"

		for numRecords := 1; numRecords <= 10; numRecords++ {
			_, err := s.AddRecord(topicName, []byte("this be record"))
			require.NoError(t, err)

			gotMetadata, err := s.Metadata(topicName)
			require.NoError(t, err)
			t0 := time.Now()

			require.Equal(t, uint64(numRecords), gotMetadata.NextOffset)
			require.True(t, timey.DiffEqual(5*time.Millisecond, t0, gotMetadata.LatestCommitAt))
		}
	})
}

// TestStorageMetadataTopicNotFound verifies that ErrTopicNotFound is returned
// when attempting to read metadata from a topic that does not exist, when topic
// auto creation is turned off.
func TestStorageMetadataTopicNotFound(t *testing.T) {
	tests := map[string]struct {
		autoCreate  bool
		expectedErr error
	}{
		"no auto create": {autoCreate: false, expectedErr: seb.ErrTopicNotFound},
		"auto create":    {autoCreate: true, expectedErr: nil},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			tester.TestStorage(t, test.autoCreate, func(t *testing.T, s *storage.Storage) {
				_, err := s.Metadata("does-not-exist")
				require.ErrorIs(t, err, test.expectedErr)
			})
		})
	}
}

// TestAddRecordsHappyPath verifies that AddRecords adds the expected records.
func TestAddRecordsHappyPath(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, ts topic.Storage, cache *cache.Cache) {
		s := storage.New(log,
			storage.NewTopicFactory(ts, cache),
			storage.WithNullBatcher(),
			storage.WithAutoCreateTopic(true),
		)

		const topicName = "topic"
		expectedRecords := tester.MakeRandomRecordBatch(5)

		// Act
		_, err := s.AddRecords(topicName, expectedRecords)
		require.NoError(t, err)

		// Assert
		gotRecords, err := s.GetRecords(context.Background(), topicName, 0, 9999, 0)
		require.NoError(t, err)

		require.Equal(t, expectedRecords, gotRecords)
	})
}

// TestAddRecordHappyPath verifies that AddRecord adds the expected records.
func TestAddRecordHappyPath(t *testing.T) {
	tester.TestTopicStorageAndCache(t, func(t *testing.T, ts topic.Storage, cache *cache.Cache) {
		s := storage.New(log,
			storage.NewTopicFactory(ts, cache),
			storage.WithNullBatcher(),
			storage.WithAutoCreateTopic(true),
		)

		const topicName = "topic"
		expectedRecords := tester.MakeRandomRecordBatch(5)

		// Act
		for _, record := range expectedRecords {
			_, err := s.AddRecord(topicName, record)
			require.NoError(t, err)
		}

		// Assert
		gotRecords, err := s.GetRecords(context.Background(), topicName, 0, 9999, 0)
		require.NoError(t, err)

		require.Equal(t, expectedRecords, gotRecords)
	})
}
