package sebcache_test

import (
	"errors"
	"io"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/seberr"
	"github.com/stretchr/testify/require"
)

var fillError = errors.New("partial cache write")

type partialFillStorage struct {
	sebcache.Storage
	failWrite, failRemove bool
}

func (s *partialFillStorage) Writer(key string) (io.WriteCloser, error) {
	w, err := s.Storage.Writer(key)
	if err != nil || !s.failWrite {
		return w, err
	}

	return partialFillWriter{w}, nil
}

func (s *partialFillStorage) Remove(key string) error {
	if s.failRemove {
		return fillError
	}

	return s.Storage.Remove(key)
}

type partialFillWriter struct{ io.WriteCloser }

func (w partialFillWriter) Write(p []byte) (int, error) {
	n, _ := w.WriteCloser.Write(p[:1])

	return n, fillError
}

func TestCacheFailedFillCannotBeRead(t *testing.T) {
	for _, failRemove := range []bool{false, true} {
		storage := &partialFillStorage{
			Storage:    sebcache.NewMemoryStorage(log),
			failWrite:  true,
			failRemove: failRemove,
		}
		cache, err := sebcache.New(log, storage)
		require.NoError(t, err)

		w, err := cache.Writer("batch")
		require.NoError(t, err)

		n, err := w.Write([]byte("abcdef"))
		require.Equal(t, 1, n)
		require.ErrorIs(t, err, fillError)
		require.ErrorIs(t, w.Close(), fillError)

		_, err = cache.Reader("batch")
		require.ErrorIs(t, err, seberr.ErrNotInCache)
		require.Zero(t, cache.Size())

		// A successful retry clears invalidation, even if deletion had failed.
		storage.failWrite = false
		w, err = cache.Writer("batch")
		require.NoError(t, err)

		_, err = w.Write([]byte("abcdef"))
		require.NoError(t, err)
		require.NoError(t, w.Close())

		r, err := cache.Reader("batch")
		require.NoError(t, err)

		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, []byte("abcdef"), got)
	}
}

func TestCacheAbortsFillWhenSourceFails(t *testing.T) {
	cache, err := sebcache.NewMemoryCache(log)
	require.NoError(t, err)

	w, err := cache.Writer("batch")
	require.NoError(t, err)

	_, err = w.Write([]byte("prefix"))
	require.NoError(t, err)
	require.Error(t, w.Abort())

	_, err = cache.Reader("batch")
	require.ErrorIs(t, err, seberr.ErrNotInCache)
	require.Zero(t, cache.Size())
}

func TestCacheAbortPreservesPreviouslyCommittedEntry(t *testing.T) {
	tester.TestCacheStorage(t, func(t *testing.T, storage sebcache.Storage) {
		cache, err := sebcache.New(log, storage)
		require.NoError(t, err)

		_, err = cache.Write("batch", []byte("complete"))
		require.NoError(t, err)

		w, err := cache.Writer("batch")
		require.NoError(t, err)

		_, err = w.Write([]byte("prefix"))
		require.NoError(t, err)
		require.Error(t, w.Abort())

		r, err := cache.Reader("batch")
		require.NoError(t, err)

		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.Equal(t, []byte("complete"), got)
		require.Equal(t, int64(8), cache.Size())
	})
}
