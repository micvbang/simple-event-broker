package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/micvbang/go-helpy/filepathy"
	"github.com/micvbang/go-helpy/mapy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

type cacheItem struct {
	size       int64
	accessedAt time.Time
	path       string
}

// DiskCache is a key-value store for caching data in files on the local disk.
type DiskCache struct {
	log     logger.Logger
	rootDir string
	now     func() time.Time

	mu         sync.Mutex
	cacheItems map[string]cacheItem
}

func NewDiskCacheDefault(log logger.Logger, rootDir string) (*DiskCache, error) {
	return NewDiskCacheWithNow(log, rootDir, time.Now)
}

func NewDiskCacheWithNow(log logger.Logger, rootDir string, now func() time.Time) (*DiskCache, error) {
	cacheItems := make(map[string]cacheItem, 64)

	fileWalkConfig := filepathy.WalkConfig{
		Files:     true,
		Recursive: true,
	}
	err := filepathy.Walk(rootDir, fileWalkConfig, func(path string, info os.FileInfo, err error) error {
		cacheItems[path] = cacheItem{
			size:       info.Size(),
			accessedAt: info.ModTime(),
			path:       path,
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading existing cache: %w", err)
	}

	return &DiskCache{
		log:        log,
		rootDir:    rootDir,
		cacheItems: cacheItems,
		now:        now,
	}, nil
}

func (c *DiskCache) Writer(key string) (io.WriteCloser, error) {
	log := c.log.WithField("key", key)

	log.Debugf("adding '%s'", key)

	cachePath := c.cachePath(key)

	return newCacheWriter(cachePath, func(size int64) {
		log.Debugf("adding to cache items")

		c.mu.Lock()
		defer c.mu.Unlock()

		c.cacheItems[cachePath] = cacheItem{
			size:       size,
			accessedAt: c.now(),
			path:       cachePath,
		}
	})
}

func (c *DiskCache) Write(key string, bs []byte) (int, error) {
	wtr, err := c.Writer(key)
	if err != nil {
		return 0, fmt.Errorf("creating writer: %w", err)
	}
	defer wtr.Close()

	return wtr.Write(bs)
}

// EvictLeastRecentlyUsed removes items from the cache until it has the given maxSize.
// The least recently used items will be removed first.
func (c *DiskCache) EvictLeastRecentlyUsed(maxSize int64) error {
	log := c.log.WithField("maxSize", maxSize)

	c.mu.Lock()
	defer c.mu.Unlock()

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
		log.Debugf("deleting all items last accessed at <= %v", cacheItemsToDelete[0])
	}

	bytesDeleted := int64(0)
	itemsDeleted := 0
	for _, item := range cacheItemsToDelete {
		log.Debugf("deleting %s (%d bytes)", item.path, item.size)
		err := os.Remove(item.path)
		if err != nil {
			log.Errorf("deleting '%s': %w", err)
			return fmt.Errorf("deleting %s: %w", item.path, err)
		}

		itemsDeleted += 1
		bytesDeleted += item.size
		delete(c.cacheItems, item.path)
	}
	log.Infof("deleted %d items (%d bytes)", itemsDeleted, bytesDeleted)

	return nil
}

func (c *DiskCache) Size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.log.Debugf("computing size of %d items", len(c.cacheItems))
	size := int64(0)
	for _, item := range c.cacheItems {
		size += item.size
	}
	return size
}

func (c *DiskCache) Reader(key string) (io.ReadSeekCloser, error) {
	log := c.log.WithField("key", key)

	cachePath := c.cachePath(key)
	f, err := os.Open(cachePath)
	if err != nil {
		log.Debugf("miss", key)
		return nil, errors.Join(ErrNotInCache, fmt.Errorf("opening record batch '%s': %w", key, err))
	}
	log.Debugf("hit", key)

	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.cacheItems[cachePath]
	if !ok {
		log.Debugf("not found in cache items, adding")
		fileInfo, err := os.Stat(cachePath)
		if err == nil {
			item = cacheItem{
				size:       fileInfo.Size(),
				accessedAt: fileInfo.ModTime(),
			}
		}
	}
	item.accessedAt = c.now()
	c.cacheItems[cachePath] = item

	return f, nil
}

func (c *DiskCache) cachePath(key string) string {
	return path.Join(c.rootDir, key)
}

func newCacheWriter(destPath string, reportFileSize func(size int64)) (*cacheWriter, error) {
	tmpFile, err := os.CreateTemp("", "seb_*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}

	return &cacheWriter{
		tmpFile:        tmpFile,
		destPath:       destPath,
		reportFileSize: reportFileSize,
	}, nil
}

type cacheWriter struct {
	tmpFile  *os.File
	destPath string
	size     int64

	reportFileSize func(int64)
}

func (cw *cacheWriter) Write(bs []byte) (int, error) {
	n, err := cw.tmpFile.Write(bs)
	cw.size += int64(n)
	return n, err
}

func (cw cacheWriter) Close() error {
	err := cw.tmpFile.Close()
	if err != nil {
		return fmt.Errorf("closing cacheWriter file: %w", err)
	}

	cacheDir := path.Dir(cw.destPath)
	err = os.MkdirAll(cacheDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("creating cache dirs '%s': %w", cacheDir, err)
	}

	err = os.Rename(cw.tmpFile.Name(), cw.destPath)
	if err != nil {
		return fmt.Errorf("moving %s to %s: %w", cw.tmpFile.Name(), cw.destPath, err)
	}

	cw.reportFileSize(cw.size)

	return nil
}
