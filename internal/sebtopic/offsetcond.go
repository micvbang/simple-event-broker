package sebtopic

import (
	"container/list"
	"context"
	"sync"
)

// OffsetCond makes it easy to manage callers that wish to wait for a specific
// offset to become available.
type OffsetCond struct {
	mu            sync.Mutex
	waiting       *list.List
	currentOffset uint64
}

func NewOffsetCond(offset uint64) *OffsetCond {
	return &OffsetCond{
		currentOffset: offset,
		waiting:       list.New(),
	}
}

type wait struct {
	offset uint64
	ch     chan struct{}
}

// Broadcast unblocks any callers that have called Wait() with an offset
// that is lower or equal to the given offset.
func (c *OffsetCond) Broadcast(offset uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.currentOffset = offset

	for el := c.waiting.Front(); el != nil; {
		next := el.Next()

		v := el.Value.(wait)
		if offset >= v.offset {
			close(v.ch)
			c.waiting.Remove(el)
		}

		el = next
	}
}

// Wait blocks until the given offset has been reached. Can only return errors
// from the context expiring or nil.
func (c *OffsetCond) Wait(ctx context.Context, offset uint64) error {
	c.mu.Lock()
	if offset <= c.currentOffset {
		c.mu.Unlock()
		return nil
	}

	ch := make(chan struct{})
	c.waiting.PushBack(wait{
		offset: offset,
		ch:     ch,
	})
	c.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *OffsetCond) Waiting() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waiting.Len()
}
