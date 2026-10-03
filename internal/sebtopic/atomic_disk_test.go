package sebtopic_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/stretchr/testify/require"
)

var diskStageFailure = errors.New("injected staged batch failure")

type stageFailureStorage struct{ sebtopic.Storage }

func (s stageFailureStorage) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	w, err := s.Storage.Writer(ctx, key)
	if err != nil {
		return nil, err
	}

	return stageFailureWriter{w}, nil
}

type stageFailureWriter struct{ io.WriteCloser }

func (w stageFailureWriter) Write(p []byte) (int, error) {
	n, _ := w.WriteCloser.Write(p[:1])

	return n, diskStageFailure
}

func (w stageFailureWriter) Abort() error {
	return w.WriteCloser.(interface{ Abort() error }).Abort()
}

type stageFailureCompression struct {
	sebtopic.Gzip
	failure string
}

func (c stageFailureCompression) NewWriter(w io.Writer) (io.WriteCloser, error) {
	if c.failure == "compression creation" {
		return nil, diskStageFailure
	}

	compressed, err := c.Gzip.NewWriter(w)
	if err != nil {
		return nil, err
	}

	return stageFailureCloser{compressed}, nil
}

type stageFailureCloser struct{ io.WriteCloser }

func (w stageFailureCloser) Close() error {
	return errors.Join(w.WriteCloser.Close(), diskStageFailure)
}

func TestFailedDiskBatchDoesNotPreventTopicRestart(t *testing.T) {
	for _, failure := range []string{"partial write", "compression creation", "compression close"} {
		t.Run(failure, func(t *testing.T) {
			root := t.TempDir()
			backing := sebtopic.NewDiskStorage(log, root)
			cache, err := sebcache.NewMemoryCache(log)
			require.NoError(t, err)

			topic, err := sebtopic.New(log, backing, "topic", cache)
			require.NoError(t, err)

			_, err = topic.AddRecords(sebrecords.NewBatch([]uint32{7}, []byte("durable")))
			require.NoError(t, err)

			var failingStorage sebtopic.Storage = backing
			var compression sebtopic.Compress = sebtopic.Gzip{}
			if failure == "partial write" {
				failingStorage = stageFailureStorage{backing}
			} else {
				compression = stageFailureCompression{failure: failure}
			}

			topic, err = sebtopic.New(log, failingStorage, "topic", cache, sebtopic.WithCompress(compression))
			require.NoError(t, err)

			_, err = topic.AddRecords(sebrecords.NewBatch([]uint32{1}, []byte("x")))
			require.ErrorIs(t, err, diskStageFailure)
			require.Equal(t, uint64(1), topic.NextOffset())

			entries, err := os.ReadDir(filepath.Join(root, "topic"))
			require.NoError(t, err)
			require.Len(t, entries, 1, "failed stage must be removed without publishing a tail batch")

			freshCache, err := sebcache.NewMemoryCache(log)
			require.NoError(t, err)

			reopened, err := sebtopic.New(log, backing, "topic", freshCache)
			require.NoError(t, err)

			got := tester.NewBatch(1, 64)
			require.NoError(t, reopened.ReadRecords(context.Background(), &got, 0, 1, 0))
			require.Equal(t, []byte("durable"), got.Data)

			offsets, err := reopened.AddRecords(sebrecords.NewBatch([]uint32{1}, []byte("x")))
			require.NoError(t, err)
			require.Equal(t, []uint64{1}, offsets)
		})
	}
}

func TestUncommittedDiskStageIsInvisibleOnRestart(t *testing.T) {
	backing := sebtopic.NewDiskStorage(log, t.TempDir())
	cache, err := sebcache.NewMemoryCache(log)
	require.NoError(t, err)

	topic, err := sebtopic.New(log, backing, "topic", cache)
	require.NoError(t, err)

	_, err = topic.AddRecords(sebrecords.NewBatch([]uint32{1}, []byte("A")))
	require.NoError(t, err)

	stage, err := backing.Writer(context.Background(), sebtopic.RecordBatchKey("topic", 1))
	require.NoError(t, err)
	defer stage.(interface{ Abort() error }).Abort()

	_, err = stage.Write([]byte("incomplete"))
	require.NoError(t, err)

	freshCache, err := sebcache.NewMemoryCache(log)
	require.NoError(t, err)

	reopened, err := sebtopic.New(log, backing, "topic", freshCache)
	require.NoError(t, err)
	require.Equal(t, uint64(1), reopened.NextOffset())
}

func TestDiskBatchReplacementPublishesOnlyOnClose(t *testing.T) {
	backing := sebtopic.NewDiskStorage(log, t.TempDir())
	key := sebtopic.RecordBatchKey("topic", 0)
	w, err := backing.Writer(context.Background(), key)
	require.NoError(t, err)

	tester.WriteAndClose(t, w, []byte("old"))
	w, err = backing.Writer(context.Background(), key)
	require.NoError(t, err)

	_, err = w.Write([]byte("new complete"))
	require.NoError(t, err)

	r, err := backing.Reader(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, []byte("old"), tester.ReadAndClose(t, r))
	require.NoError(t, w.Close())

	r, err = backing.Reader(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, []byte("new complete"), tester.ReadAndClose(t, r))
}
