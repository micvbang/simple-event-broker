package sebtopic_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/stretchr/testify/require"
)

// TestFetcherSharesDownloads verifies that concurrent calls to Fetch() for the
// same key share a single download.
func TestFetcherSharesDownloads(t *testing.T) {
	const callers = 10
	fetcher := sebtopic.NewFetcher(callers, time.Minute)

	downloads := atomic.Int32{}
	release := make(chan struct{})
	download := func(ctx context.Context) error {
		downloads.Add(1)
		<-release
		return nil
	}

	errs := make(chan error, callers)
	for range callers {
		go func() {
			errs <- fetcher.Fetch(context.Background(), "key", download)
		}()
	}

	// give all callers a chance to join the download before it completes
	time.Sleep(10 * time.Millisecond)
	close(release)

	// Act, Assert
	for range callers {
		require.NoError(t, <-errs)
	}
	require.Equal(t, int32(1), downloads.Load())
}

// TestFetcherLimitsConcurrentDownloads verifies that at most maxConcurrent
// downloads run at the same time, across both Fetch() and Prefetch().
func TestFetcherLimitsConcurrentDownloads(t *testing.T) {
	const (
		maxConcurrent = 3
		keys          = 10
	)
	fetcher := sebtopic.NewFetcher(maxConcurrent, time.Minute)

	var (
		mu        sync.Mutex
		active    int
		maxActive int
	)
	release := make(chan struct{})
	download := func(ctx context.Context) error {
		mu.Lock()
		active += 1
		maxActive = max(maxActive, active)
		mu.Unlock()

		<-release

		mu.Lock()
		active -= 1
		mu.Unlock()
		return nil
	}

	errs := make(chan error, keys)
	for i := range keys {
		key := fmt.Sprintf("key-%d", i)
		if i%2 == 0 {
			fetcher.Prefetch(key, download)
			continue
		}

		go func() {
			errs <- fetcher.Fetch(context.Background(), key, download)
		}()
	}

	// Act
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return active == maxConcurrent
	}, time.Second, time.Millisecond)

	// give any excess downloads a chance to (wrongly) start
	time.Sleep(10 * time.Millisecond)
	close(release)

	// Assert
	for range keys / 2 {
		require.NoError(t, <-errs)
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, maxConcurrent, maxActive)
}

// TestFetcherCallerContextExpired verifies that Fetch() returns when the
// caller's context expires, and that the download continues for others to use.
func TestFetcherCallerContextExpired(t *testing.T) {
	fetcher := sebtopic.NewFetcher(1, time.Minute)

	downloads := atomic.Int32{}
	release := make(chan struct{})
	download := func(ctx context.Context) error {
		downloads.Add(1)
		<-release
		return ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // NOTE: canceled immediately

	// Act
	err := fetcher.Fetch(ctx, "key", download)
	require.ErrorIs(t, err, context.Canceled)

	errs := make(chan error)
	go func() {
		errs <- fetcher.Fetch(context.Background(), "key", download)
	}()

	time.Sleep(10 * time.Millisecond)
	close(release)

	// Assert
	require.NoError(t, <-errs)
	require.Equal(t, int32(1), downloads.Load())
}

// TestFetcherRetriesFailedDownloads verifies that a failed download is returned
// to its callers, and that it's attempted again on the next call to Fetch().
func TestFetcherRetriesFailedDownloads(t *testing.T) {
	fetcher := sebtopic.NewFetcher(1, time.Minute)

	expectedErr := fmt.Errorf("download failed")
	downloads := 0
	download := func(ctx context.Context) error {
		downloads += 1
		if downloads == 1 {
			return expectedErr
		}
		return nil
	}

	// Act, Assert
	err := fetcher.Fetch(context.Background(), "key", download)
	require.ErrorIs(t, err, expectedErr)

	err = fetcher.Fetch(context.Background(), "key", download)
	require.NoError(t, err)
	require.Equal(t, 2, downloads)
}

// TestFetcherTimeout verifies that downloads are given a context that expires
// after the Fetcher's timeout.
func TestFetcherTimeout(t *testing.T) {
	fetcher := sebtopic.NewFetcher(1, 10*time.Millisecond)

	download := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}

	// Act
	err := fetcher.Fetch(context.Background(), "key", download)

	// Assert
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
