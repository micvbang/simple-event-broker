package sebcache

import (
	"io"
	"sync"
	"time"

	"github.com/micvbang/go-helpy/bytey"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/nops"
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

	mu    sync.Mutex
	items map[string]memoryCacheItem
}

func NewMemoryStorage(log logger.Logger) *MemoryCache {
	return &MemoryCache{
		log:   log,
		now:   time.Now,
		items: make(map[string]memoryCacheItem, 64),
	}
}

func (mc *MemoryCache) Reader(key string) (io.ReadSeekCloser, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	item, ok := mc.items[key]
	if !ok {
		return nil, seb.ErrNotInCache
	}
	item.accessedAt = mc.now()
	mc.items[key] = item

	// in case there are multiple callers asking for the same file, we need to
	// provide them with different readers as they would otherwise race for the
	// offset.
	buf := bytey.NewBuffer(item.buf.Bytes())
	return nops.NopReadSeekCloser(buf), nil
}

func (mc *MemoryCache) Writer(key string) (io.WriteCloser, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	buf := bytey.NewBuffer(make([]byte, 0, 4096))
	mc.items[key] = memoryCacheItem{
		buf:        buf,
		accessedAt: mc.now(),
	}

	return nops.NopWriteCloser(buf), nil
}

func (mc *MemoryCache) Remove(key string) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	delete(mc.items, key)
	return nil
}

func (mc *MemoryCache) List() (map[string]CacheItem, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

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
	mc.mu.Lock()
	defer mc.mu.Unlock()

	item, ok := mc.items[key]
	if !ok {
		return CacheItem{}, seb.ErrNotInCache
	}

	return CacheItem{
		Size:       int64(item.buf.Len()),
		AccessedAt: item.accessedAt,
		Key:        key,
	}, nil
}
