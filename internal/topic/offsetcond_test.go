package topic_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/micvbang/go-helpy/uint64y"
	"github.com/micvbang/simple-event-broker/internal/topic"
	"github.com/stretchr/testify/require"
)

// TestOffsetCondDoesNotWaitWhenOffsetHighEnough verifies that Wait() does not
// block if the existing offset is higher than the one that should be waited
// for.
// NOTE: test will time out if Wait() does not return.
func TestOffsetCondDoesNotWaitWhenOffsetHighEnough(t *testing.T) {
	existingOffset := uint64(10)
	offsetCond := topic.NewOffsetCond(existingOffset)

	// Act, assert
	err := offsetCond.Wait(context.Background(), existingOffset-1)
	require.NoError(t, err)
}

// TestOffsetCondUpdatesInternalOffset verifies that Broadcast sets the internal
// state s.t. following calls to Wait() will not block if they are waiting for
// an offset that has already been seen.
func TestOffsetCondUpdatesInternalOffset(t *testing.T) {
	offsetCond := topic.NewOffsetCond(0)
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
	offsetCond := topic.NewOffsetCond(0)

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

	offsetCond := topic.NewOffsetCond(0)

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
	offsetCond := topic.NewOffsetCond(0)

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
