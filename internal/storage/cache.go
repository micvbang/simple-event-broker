package storage

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/micvbang/go-helpy/mapy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

type CacheStorage interface {
	Reader(key string) (io.ReadSeekCloser, error)
	Writer(key string) (io.WriteCloser, error)
	Remove(key string) error
	List() (map[string]cacheItem, error)
	SizeOf(key string) (cacheItem, error)
}

type Cache struct {
	log     logger.Logger
	storage CacheStorage
	now     func() time.Time

	mu         sync.Mutex
	cacheItems map[string]cacheItem
}

func NewCacheDefault(log logger.Logger, cacheStorage CacheStorage) (*Cache, error) {
	return NewCacheWithNow(log, cacheStorage, time.Now)
}

func NewCacheWithNow(log logger.Logger, cacheStorage CacheStorage, now func() time.Time) (*Cache, error) {
	cacheItems, err := cacheStorage.List()
	if err != nil {
		return nil, fmt.Errorf("listing existing files: %w", err)
	}

	return &Cache{
		log:        log,
		storage:    cacheStorage,
		cacheItems: cacheItems,
		now:        now,
	}, nil
}

func (c *Cache) Writer(key string) (io.WriteCloser, error) {
	log := c.log.WithField("key", key)

	w, err := c.storage.Writer(key)
	if err != nil {
		return nil, err
	}

	return newWriteCloseWrapper(w, func(size int64) {
		log.Debugf("adding to cache items")

		c.mu.Lock()
		defer c.mu.Unlock()

		c.cacheItems[key] = cacheItem{
			size:       size,
			accessedAt: c.now(),
			key:        key,
		}

	}), nil
}

func (c *Cache) Write(key string, bs []byte) (int, error) {
	wtr, err := c.Writer(key)
	if err != nil {
		return 0, fmt.Errorf("creating writer: %w", err)
	}
	defer wtr.Close()

	return wtr.Write(bs)
}

func (c *Cache) Reader(key string) (io.ReadSeekCloser, error) {
	log := c.log.WithField("key", key)

	r, err := c.storage.Reader(key)
	if err != nil {
		return nil, fmt.Errorf("reading from cache storage: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.cacheItems[key]
	if !ok {
		log.Debugf("not found in cache items, adding")
		newItem, err := c.storage.SizeOf(key)
		if err == nil {
			item = newItem
		}
	}
	item.accessedAt = c.now()
	c.cacheItems[key] = item

	return r, nil
}

func (c *Cache) Size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.log.Debugf("computing size of %d items", len(c.cacheItems))
	size := int64(0)
	for _, item := range c.cacheItems {
		size += item.size
	}
	return size
}

func (c *Cache) EvictLeastRecentlyUsed(maxSize int64) error {
	log := c.log.WithField("maxSize", maxSize)

	c.mu.Lock()

	cacheItems := mapy.Values(c.cacheItems)
	sort.Slice(cacheItems, func(i, j int) bool {
		// NOTE: sorts most recently used first
		return cacheItems[j].accessedAt.Before(cacheItems[i].accessedAt)
	})

	curSize := int64(0)
	var cacheItemsToDelete []cacheItem
	for i, item := range cacheItems {
		curSize += item.size
		if curSize > maxSize {
			cacheItemsToDelete = cacheItems[i:]
			break
		}
	}

	if len(cacheItemsToDelete) > 0 {
		log.Debugf("deleting all items last accessed at <= %s", cacheItemsToDelete[0].accessedAt)
	}

	bytesDeleted := int64(0)
	itemsDeleted := 0
	for _, item := range cacheItemsToDelete {
		log.Debugf("deleting %s (%d bytes)", item.key, item.size)
		err := c.storage.Remove(item.key)
		if err != nil {
			log.Errorf("deleting '%s': %w", err)
			return fmt.Errorf("deleting %s: %w", item.key, err)
		}

		itemsDeleted += 1
		bytesDeleted += item.size
		delete(c.cacheItems, item.key)
	}
	c.mu.Unlock()

	log.Infof("deleted %d items (%d bytes) -> cache is now %d bytes", itemsDeleted, bytesDeleted, c.Size())

	return nil

}

type writeCloseWrapper struct {
	wc         io.WriteCloser
	size       int64
	afterClose func(int64)
}

func newWriteCloseWrapper(wc io.WriteCloser, afterClose func(int64)) *writeCloseWrapper {
	return &writeCloseWrapper{
		wc:         wc,
		afterClose: afterClose,
	}
}

func (w *writeCloseWrapper) Write(bs []byte) (int, error) {
	n, err := w.wc.Write(bs)
	w.size += int64(n)
	return n, err
}

func (w *writeCloseWrapper) Close() error {
	err := w.wc.Close()
	if err != nil {
		return fmt.Errorf("closing writeCloseWrapper file: %w", err)
	}

	w.afterClose(w.size)

	return nil
}
