package storage

import (
	"fmt"
	"io"
	"time"

	"github.com/micvbang/go-helpy/bytey"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

type memoryCacheItem struct {
	buf        *bytey.Buffer
	accessedAt time.Time
}

// MemoryCache caches data in memory.
// NOTE: MemoryCache assumes that the caller is handling thread safety!
type MemoryCache struct {
	log logger.Logger
	now func() time.Time

	items map[string]memoryCacheItem
}

func NewMemoryCache(log logger.Logger) *MemoryCache {
	return &MemoryCache{
		log:   log,
		now:   time.Now,
		items: make(map[string]memoryCacheItem, 64),
	}
}

func (mc *MemoryCache) Reader(key string) (io.ReadSeekCloser, error) {
	item, ok := mc.items[key]
	if !ok {
		return nil, ErrNotInCache
	}
	item.accessedAt = mc.now()
	mc.items[key] = item

	// Caller expects a new Reader into the item, so we need to reset the offset.
	_, err := item.buf.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking to start of buffer: %w", err)
	}

	return NopReadSeekCloser(item.buf), nil
}

func (mc *MemoryCache) Writer(key string) (io.WriteCloser, error) {
	buf := bytey.NewBuffer(nil)

	mc.items[key] = memoryCacheItem{
		buf:        buf,
		accessedAt: mc.now(),
	}

	return NopWriteCloser(buf), nil
}

func (mc *MemoryCache) Remove(key string) error {
	delete(mc.items, key)
	return nil
}

func (mc *MemoryCache) List() (map[string]CacheItem, error) {
	cacheItems := make(map[string]CacheItem, len(mc.items))
	for key, item := range mc.items {
		cacheItems[key] = CacheItem{
			Size:       int64(item.buf.Len()),
			AccessedAt: item.accessedAt,
			Key:        key,
		}
	}
	return cacheItems, nil
}

func (mc *MemoryCache) SizeOf(key string) (CacheItem, error) {
	item, ok := mc.items[key]
	if !ok {
		return CacheItem{}, ErrNotInCache
	}

	return CacheItem{
		Size:       int64(item.buf.Len()),
		AccessedAt: item.accessedAt,
		Key:        key,
	}, nil
}
