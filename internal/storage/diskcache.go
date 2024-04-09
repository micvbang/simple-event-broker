package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/micvbang/go-helpy/filepathy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

type cacheItem struct {
	size      int64
	createdAt time.Time
	usedAt    time.Time
}

// DiskCache is used to cache RecordBatches on the local disk.
type DiskCache struct {
	log     logger.Logger
	rootdir string

	mu         sync.Mutex
	cacheItems map[string]cacheItem
}

func NewDiskCache(log logger.Logger, rootDir string) (*DiskCache, error) {
	cacheItems := make(map[string]cacheItem, 64)

	fileWalkConfig := filepathy.WalkConfig{
		Files:     true,
		Recursive: true,
	}
	err := filepathy.Walk(rootDir, fileWalkConfig, func(path string, info os.FileInfo, err error) error {
		cacheItems[path] = cacheItem{
			size:      info.Size(),
			createdAt: info.ModTime(),
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading existing cache: %w", err)
	}

	return &DiskCache{log: log, rootdir: rootDir, cacheItems: cacheItems}, nil
}

// TODO: somehow to throw away "old" (no longer needed) data

func (c *DiskCache) Writer(recordBatchPath string) (io.WriteCloser, error) {
	log := c.log.WithField("recordBatchPath", recordBatchPath)

	log.Debugf("adding '%s'", recordBatchPath)
	tmpFile, err := os.CreateTemp("", "seb_*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}

	cachePath := c.cachePath(recordBatchPath)

	return &cacheWriter{
		tmpFile:  tmpFile,
		destPath: cachePath,
		reportFileSize: func(size int64) {
			log.Debugf("adding to cache items")

			c.mu.Lock()
			defer c.mu.Unlock()

			c.cacheItems[cachePath] = cacheItem{
				size:      size,
				createdAt: time.Now(),
			}
		},
	}, nil
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

func (c *DiskCache) Reader(recordBatchPath string) (io.ReadSeekCloser, error) {
	log := c.log.WithField("recordBatchPath", recordBatchPath)

	cachePath := c.cachePath(recordBatchPath)
	f, err := os.Open(cachePath)
	if err != nil {
		return nil, errors.Join(ErrNotInCache, fmt.Errorf("opening record batch '%s': %w", recordBatchPath, err))
	}

	now := time.Now()
	log.Debugf("hit for '%s', updating used at", recordBatchPath)

	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.cacheItems[cachePath]
	if !ok {
		log.Debugf("not found in cache items, adding")
		fileInfo, err := os.Stat(cachePath)
		if err == nil {
			item = cacheItem{
				size:      fileInfo.Size(),
				createdAt: fileInfo.ModTime(),
			}
		}
	}
	item.usedAt = now
	c.cacheItems[cachePath] = item

	return f, nil
}

func (c *DiskCache) cachePath(recordBatchPath string) string {
	return path.Join(c.rootdir, recordBatchPath)
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
