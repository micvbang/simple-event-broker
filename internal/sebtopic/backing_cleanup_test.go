package sebtopic_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/tester"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/stretchr/testify/require"
)

var cacheFillFailure = errors.New("injected cache fill failure")

type cleanupReadCloser struct {
	io.ReadCloser
	closed *int
}

func (r cleanupReadCloser) Close() error {
	*r.closed++

	return r.ReadCloser.Close()
}

type cleanupStorage struct {
	sebtopic.Storage
	opens, closes int
}

func (s *cleanupStorage) Reader(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := s.Storage.Reader(ctx, key)
	if err != nil {
		return nil, err
	}

	s.opens++

	return cleanupReadCloser{r, &s.closes}, nil
}

type cleanupCompression struct {
	sebtopic.Gzip
	opens, closes int
}

func (c *cleanupCompression) NewReader(r io.Reader) (io.ReadCloser, error) {
	rdr, err := c.Gzip.NewReader(r)
	if err != nil {
		return nil, err
	}

	c.opens++

	return cleanupReadCloser{rdr, &c.closes}, nil
}

type cleanupCache struct {
	sebcache.Storage
	failure  string
	attempts int
}

func (c *cleanupCache) Writer(key string) (io.WriteCloser, error) {
	c.attempts++
	if c.failure == "open" {
		return nil, cacheFillFailure
	}

	w, err := c.Storage.Writer(key)
	if err != nil {
		return nil, err
	}

	return cleanupCacheWriter{w, c.failure}, nil
}

type cleanupCacheWriter struct {
	io.WriteCloser
	failure string
}

func (w cleanupCacheWriter) Write(p []byte) (int, error) {
	if w.failure == "write" {
		n, _ := w.WriteCloser.Write(p[:1])

		return n, cacheFillFailure
	}

	return w.WriteCloser.Write(p)
}

func (w cleanupCacheWriter) Close() error {
	err := w.WriteCloser.Close()
	if w.failure == "close" {
		return cacheFillFailure
	}

	return err
}

func TestTopicClosesBackingReadersOnCacheFillFailures(t *testing.T) {
	for _, failure := range []string{"open", "write", "close", "none"} {
		t.Run(failure, func(t *testing.T) {
			backing := &cleanupStorage{Storage: sebtopic.NewMemoryStorage(log)}
			cs := &cleanupCache{
				Storage: sebcache.NewMemoryStorage(log),
				failure: failure,
			}
			cache, err := sebcache.New(log, cs)
			require.NoError(t, err)

			compression := &cleanupCompression{}
			topic, err := sebtopic.New(log, backing, "topic", cache, sebtopic.WithCompress(compression))
			require.NoError(t, err)

			_, err = topic.AddRecords(sebrecords.NewBatch([]uint32{1}, []byte("A")))
			require.NoError(t, err)

			got := tester.NewBatch(1, 64)
			require.NoError(t, topic.ReadRecords(context.Background(), &got, 0, 1, 0))
			require.Equal(t, []byte("A"), got.Data)

			got.Reset()
			require.NoError(t, topic.ReadRecords(context.Background(), &got, 0, 1, 0))
			require.Equal(t, []byte("A"), got.Data)
			require.Positive(t, cs.attempts)
			require.Positive(t, backing.opens)
			require.Equal(t, backing.opens, backing.closes)
			require.Equal(t, compression.opens, compression.closes)

			_, err = sebtopic.New(log, backing, "topic", cache, sebtopic.WithCompress(compression))
			require.NoError(t, err)
			require.Equal(t, backing.opens, backing.closes)
			require.Equal(t, compression.opens, compression.closes)
		})
	}
}
