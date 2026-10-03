package sebcache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/micvbang/go-helpy/mapy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/seberr"
)

type Storage interface {
	Reader(key string) (io.ReadSeekCloser, error)
	Writer(key string) (io.WriteCloser, error)
	Remove(key string) error
	List() (map[string]CacheItem, error)
	SizeOf(key string) (CacheItem, error)
}

type Cache struct {
	log     logger.Logger
	storage Storage
	now     func() time.Time

	mu          sync.Mutex
	cacheItems  map[string]CacheItem
	invalidKeys map[string]struct{}
}

// NewDiskCache returns a new Cache with DiskStorage.
func NewDiskCache(log logger.Logger, rootDir string) (*Cache, error) {
	diskStorage, err := NewDiskStorage(log.Name("disk storage"), rootDir)
	if err != nil {
		return nil, fmt.Errorf("creating disk storage: %w", err)
	}

	return New(log, diskStorage)
}

// NewMemoryCache returns a new Cache with MemoryStorage.
func NewMemoryCache(log logger.Logger) (*Cache, error) {
	memoryStorage := NewMemoryStorage(log.Name("memory cache"))

	return New(log, memoryStorage)
}

func New(log logger.Logger, cacheStorage Storage) (*Cache, error) {
	return NewCacheWithNow(log, cacheStorage, time.Now)
}

func NewCacheWithNow(log logger.Logger, cacheStorage Storage, now func() time.Time) (*Cache, error) {
	cacheItems, err := cacheStorage.List()
	if err != nil {
		return nil, fmt.Errorf("listing existing files: %w", err)
	}

	return &Cache{
		log:         log,
		storage:     cacheStorage,
		cacheItems:  cacheItems,
		invalidKeys: make(map[string]struct{}),
		now:         now,
	}, nil
}

// WriteAbortCloser can discard a cache fill when its source reader fails.
type WriteAbortCloser interface {
	io.WriteCloser
	Abort() error
}

func (c *Cache) Writer(key string) (WriteAbortCloser, error) {
	w, err := c.storage.Writer(key)
	if err != nil {
		return nil, err
	}

	return &writeCloseWrapper{
		wc: w,
		finish: func(size int64, writeErr error) error {
			// Readers cannot observe a partial publication between Close and removal.
			c.mu.Lock()
			defer c.mu.Unlock()

			if writeErr != nil {
				if aborter, ok := w.(interface{ Abort() error }); ok {
					return errors.Join(writeErr, aborter.Abort())
				}
			}

			err := errors.Join(writeErr, w.Close())
			if err != nil {
				delete(c.cacheItems, key)
				c.invalidKeys[key] = struct{}{}
				removeErr := c.storage.Remove(key)
				if removeErr == nil || errors.Is(removeErr, os.ErrNotExist) {
					delete(c.invalidKeys, key)
				}

				return errors.Join(err, removeErr)
			}

			delete(c.invalidKeys, key)
			c.cacheItems[key] = CacheItem{
				Size:       size,
				AccessedAt: c.now(),
				Key:        key,
			}

			return nil
		},
	}, nil
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

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, invalid := c.invalidKeys[key]; invalid {
		return nil, seberr.ErrNotInCache
	}

	r, err := c.storage.Reader(key)
	if err != nil {
		return nil, fmt.Errorf("reading from cache storage: %w", err)
	}

	item, ok := c.cacheItems[key]
	if !ok {
		log.Debugf("not found in cache items, adding")
		newItem, err := c.storage.SizeOf(key)
		if err == nil {
			item = newItem
		} else {
			// NOTE: should only happen if
			// 1) the file was deleted between opening it and getting its size
			// 2) the storage implementation is broken somehow.
			r.Close()
			return nil, seberr.ErrNotInCache
		}
	}

	item.AccessedAt = c.now()
	c.cacheItems[key] = item

	return r, nil
}

func (c *Cache) Size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.size()
}

// size computes the number of bytes in c.cacheItems.
// NOTE: you must hold c.mu lock when calling this method!
func (c *Cache) size() int64 {
	c.log.Debugf("computing size of %d items", len(c.cacheItems))
	size := int64(0)
	for _, item := range c.cacheItems {
		size += item.Size
	}
	return size
}

func (c *Cache) EvictLeastRecentlyUsed(maxSize int64) error {
	log := c.log.WithField("maxSize", maxSize)

	c.mu.Lock()

	cacheItems := mapy.Values(c.cacheItems)
	sort.Slice(cacheItems, func(i, j int) bool {
		// NOTE: sorts most recently used first
		return cacheItems[j].AccessedAt.Before(cacheItems[i].AccessedAt)
	})

	curSize := int64(0)
	var cacheItemsToDelete []CacheItem
	for i, item := range cacheItems {
		curSize += item.Size
		if curSize > maxSize {
			cacheItemsToDelete = cacheItems[i:]
			break
		}
	}

	if len(cacheItemsToDelete) > 0 {
		log.Debugf("deleting all items last accessed at <= %s", cacheItemsToDelete[0].AccessedAt)
	}

	// Remove entries from the cache so that we can release the lock and
	// continue to actually delete the files
	for _, item := range cacheItemsToDelete {
		delete(c.cacheItems, item.Key)
	}
	cacheSize := c.size()
	c.mu.Unlock()

	// NOTE: if c.storage.Remove fails and a file actually remains on the disk, it will
	// remain there forever.
	bytesDeleted := int64(0)
	itemsDeleted := 0
	var removeErrs error = nil
	for _, item := range cacheItemsToDelete {
		log.Debugf("deleting %s (%d bytes)", item.Key, item.Size)
		err := c.storage.Remove(item.Key)
		if err != nil {
			log.Errorf("deleting '%s': %w", item.Key, err)
			removeErrs = errors.Join(removeErrs, fmt.Errorf("deleting %s: %w", item.Key, err))
			continue
		}

		itemsDeleted += 1
		bytesDeleted += item.Size
	}

	log.Infof("deleted %d items (%d bytes) -> cache is now %d bytes", itemsDeleted, bytesDeleted, cacheSize)

	return removeErrs

}

type writeCloseWrapper struct {
	wc       io.WriteCloser
	size     int64
	writeErr error
	closed   bool
	closeErr error
	finish   func(int64, error) error
}

func (w *writeCloseWrapper) Write(bs []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}

	if w.writeErr != nil {
		return 0, w.writeErr
	}

	n, err := w.wc.Write(bs)
	w.size += int64(n)
	if n != len(bs) && err == nil {
		err = io.ErrShortWrite
	}

	w.writeErr = err

	return n, err
}

func (w *writeCloseWrapper) Close() error {
	if !w.closed {
		w.closed = true
		w.closeErr = w.finish(w.size, w.writeErr)
	}

	return w.closeErr
}

func (w *writeCloseWrapper) Abort() error {
	if !w.closed {
		w.writeErr = io.ErrClosedPipe
	}

	return w.Close()
}
