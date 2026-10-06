package sebtopic

import (
	"context"
	"sync"
	"time"
)

// Fetcher runs downloads of record batches. It ensures that
// 1) at most maxConcurrent downloads run at the same time, across all topics
// sharing the Fetcher, and
// 2) concurrent requests for the same key share a single download.
type Fetcher struct {
	sem     chan struct{}
	timeout time.Duration

	mu       sync.Mutex
	inflight map[string]*fetch
}

type fetch struct {
	done chan struct{}
	err  error // NOTE: only safe to read once done is closed
}

// NewFetcher returns a Fetcher that runs at most maxConcurrent downloads at the
// same time. timeout is the maximum amount of time a single download can take,
// including the time spent waiting for its turn.
func NewFetcher(maxConcurrent int, timeout time.Duration) *Fetcher {
	return &Fetcher{
		sem:      make(chan struct{}, maxConcurrent),
		timeout:  timeout,
		inflight: make(map[string]*fetch),
	}
}

// Fetch blocks until key has been downloaded, or until ctx expires. If ctx
// expires, the download continues so that other callers waiting for it, and
// later requests, can still benefit from it.
func (f *Fetcher) Fetch(ctx context.Context, key string, download func(context.Context) error) error {
	fe := f.start(key, download)

	select {
	case <-fe.done:
		return fe.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Prefetch starts downloading key, unless it's already being downloaded, and
// returns immediately.
func (f *Fetcher) Prefetch(key string, download func(context.Context) error) {
	f.start(key, download)
}

func (f *Fetcher) start(key string, download func(context.Context) error) *fetch {
	f.mu.Lock()
	defer f.mu.Unlock()

	fe, ok := f.inflight[key]
	if ok {
		return fe
	}

	fe = &fetch{done: make(chan struct{})}
	f.inflight[key] = fe
	go f.run(key, fe, download)

	return fe
}

func (f *Fetcher) run(key string, fe *fetch, download func(context.Context) error) {
	// NOTE: failed downloads are forgotten once they're done, allowing later
	// requests to retry them.
	defer func() {
		f.mu.Lock()
		delete(f.inflight, key)
		f.mu.Unlock()
		close(fe.done)
	}()

	// NOTE: detached from the callers' contexts; one caller giving up must not
	// fail the download for everyone else waiting for it.
	ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
	defer cancel()

	select {
	case f.sem <- struct{}{}:
	case <-ctx.Done():
		fe.err = ctx.Err()
		return
	}
	defer func() { <-f.sem }()

	fe.err = download(ctx)
}
