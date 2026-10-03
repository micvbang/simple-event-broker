package sebtopic_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/micvbang/go-helpy/uint64y"
	sebtopic "github.com/micvbang/simple-event-broker/internal/sebtopic"
	"github.com/stretchr/testify/require"
)

func TestOffsetCondRemovesCancelledWaiters(t *testing.T) {
	c := sebtopic.NewEmptyOffsetCond()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for range 1000 {
		require.ErrorIs(t, c.Wait(ctx, 0), context.Canceled)
	}

	require.Zero(t, c.Waiting(), "idle topics must not retain expired consumers")
}

func TestOffsetCondCancellationRacesWithBroadcast(t *testing.T) {
	for range 100 {
		c := sebtopic.NewEmptyOffsetCond()
		ctx, cancel := context.WithCancel(context.Background())
		returned := make(chan error, 1)

		go func() {
			returned <- c.Wait(ctx, 0)
		}()

		require.Eventually(t, func() bool {
			return c.Waiting() == 1
		}, time.Second, time.Millisecond)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()

			cancel()
		}()

		go func() {
			defer wg.Done()

			c.Broadcast(0)
		}()

		wg.Wait()
		err := <-returned
		require.True(t, err == nil || errors.Is(err, context.Canceled))
		require.Zero(t, c.Waiting())
	}
}

func TestEmptyOffsetCondWaitsForFirstCommit(t *testing.T) {
	c := sebtopic.NewEmptyOffsetCond()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	returned := make(chan error, 1)

	go func() {
		returned <- c.Wait(ctx, 0)
	}()

	require.Eventually(t, func() bool {
		return c.Waiting() == 1
	}, time.Second, time.Millisecond)

	c.Broadcast(0)
	require.NoError(t, <-returned)

	// Once offset zero exists, even a cancelled context returns immediately.
	cancel()
	require.NoError(t, c.Wait(ctx, 0))
}

// TestOffsetCondDoesNotWaitWhenOffsetHighEnough verifies that Wait() does not
// block if the existing offset is higher than the one that should be waited
// for.
// NOTE: test will time out if Wait() does not return.
func TestOffsetCondDoesNotWaitWhenOffsetHighEnough(t *testing.T) {
	existingOffset := uint64(10)
	offsetCond := sebtopic.NewOffsetCond(existingOffset)

	// Act, assert
	err := offsetCond.Wait(context.Background(), existingOffset-1)
	require.NoError(t, err)
}

// TestOffsetCondUpdatesInternalOffset verifies that Broadcast sets the internal
// state s.t. following calls to Wait() will not block if they are waiting for
// an offset that has already been seen.
func TestOffsetCondUpdatesInternalOffset(t *testing.T) {
	offsetCond := sebtopic.NewOffsetCond(0)
	offsetCond.Broadcast(100)

	// Act, assert
	returned := make(chan struct{})
	go func() {
		err := offsetCond.Wait(context.Background(), 1)
		require.NoError(t, err)
		close(returned)
	}()

	// Assert
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	select {
	case <-returned:
	case <-ctx.Done():
		t.Fatalf("expected Wait() to return")
	}
}

// TestOffsetCondWaitsUntilOffset verifies that Wait() blocks until the expected
// offset has been broadcast.
func TestOffsetCondWaitsUntilOffset(t *testing.T) {
	const waitUntilOffset = 5
	offsetCond := sebtopic.NewOffsetCond(0)

	waiting := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		close(waiting)
		err := offsetCond.Wait(context.Background(), waitUntilOffset)
		require.NoError(t, err)
		close(returned)
	}()

	// ensure that Wait() is called
	<-waiting

	for i := uint64(0); i < waitUntilOffset; i++ {
		offsetCond.Broadcast(i)
		require.False(t, chanClosed(returned, 5*time.Millisecond))
	}

	// Act
	offsetCond.Broadcast(waitUntilOffset)

	// Assert
	require.True(t, chanClosed(returned, 5*time.Millisecond))
}

// TestOffsetCondWaitsUntilContext verifies that Wait() returns when the given
// context expires.
func TestOffsetCondWaitsUntilContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	offsetCond := sebtopic.NewOffsetCond(0)

	waiting := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		close(waiting)
		err := offsetCond.Wait(ctx, 100)
		require.ErrorIs(t, err, context.Canceled)
		close(returned)
	}()

	// ensure that Wait() is called
	<-waiting

	// Act
	cancel()

	// Assert
	require.True(t, chanClosed(returned, 5*time.Millisecond))
}

// TestOffsetCondManyWaits verifies that many waiters can be unblocked at the
// same time, and that they _are_ all unblocked.
func TestOffsetCondManyWaits(t *testing.T) {
	offsetCond := sebtopic.NewOffsetCond(0)

	const numWaiters = 100

	wg := sync.WaitGroup{}
	wg.Add(numWaiters)

	for i := 0; i < numWaiters; i++ {
		go func() {
			defer wg.Done()

			err := offsetCond.Wait(context.Background(), 1+uint64y.RandomN(numWaiters))
			require.NoError(t, err)
		}()
	}

	// give waiters some time to be scheduled
	require.Eventually(t, func() bool {
		return numWaiters == offsetCond.Waiting()
	}, time.Second, time.Millisecond)

	// Act
	offsetCond.Broadcast(numWaiters + 1)

	// wait for all waiters to have returned
	wg.Wait()

	// Assert
	require.Equal(t, 0, offsetCond.Waiting())
}

func chanClosed(ch chan struct{}, d time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}
