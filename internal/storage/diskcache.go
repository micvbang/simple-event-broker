package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/micvbang/go-helpy/filepathy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

type CacheItem struct {
	Size       int64
	AccessedAt time.Time
	Key        string
}

// DiskCache is a key-value store for caching data in files on the local disk.
type DiskCache struct {
	log     logger.Logger
	rootDir string
}

func NewDiskCache(log logger.Logger, rootDir string) *DiskCache {
	if !strings.HasSuffix(rootDir, "/") {
		rootDir += "/"
	}

	return &DiskCache{
		log:     log,
		rootDir: rootDir,
	}
}

func (c *DiskCache) List() (map[string]CacheItem, error) {
	cacheItems := make(map[string]CacheItem, 64)

	fileWalkConfig := filepathy.WalkConfig{
		Files:     true,
		Recursive: true,
	}
	err := filepathy.Walk(c.rootDir, fileWalkConfig, func(path string, info os.FileInfo, err error) error {
		cacheItems[path] = CacheItem{
			Size:       info.Size(),
			AccessedAt: info.ModTime(),
			Key:        strings.TrimPrefix(path, c.rootDir),
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading existing files: %w", err)
	}

	return cacheItems, nil
}

func (c *DiskCache) Writer(key string) (io.WriteCloser, error) {
	log := c.log.WithField("key", key)

	log.Debugf("adding '%s'", key)

	cachePath, err := c.cachePath(key)
	if err != nil {
		return nil, fmt.Errorf("getting cache path of %s: %w", key, err)
	}
	return newCacheWriter(cachePath)
}

func (c *DiskCache) Remove(key string) error {
	path, err := c.cachePath(key)
	if err != nil {
		return fmt.Errorf("getting cache path of %s: %w", key, err)
	}

	return os.Remove(path)
}

func (c *DiskCache) Reader(key string) (io.ReadSeekCloser, error) {
	log := c.log.WithField("key", key)

	cachePath, err := c.cachePath(key)
	if err != nil {
		return nil, fmt.Errorf("getting cache path of %s: %w", key, err)
	}
	f, err := os.Open(cachePath)
	if err != nil {
		log.Debugf("miss")
		return nil, errors.Join(ErrNotInCache, fmt.Errorf("opening record batch '%s': %w", key, err))
	}
	log.Debugf("hit")

	return f, nil
}

func (c *DiskCache) SizeOf(key string) (CacheItem, error) {
	log := c.log.WithField("key", key)

	fileInfo, err := os.Stat(key)
	if err != nil {
		return CacheItem{}, fmt.Errorf("calling os.Stat: %w", err)
	}

	log.Debugf("found")
	return CacheItem{
		Size:       fileInfo.Size(),
		AccessedAt: fileInfo.ModTime(),
	}, nil
}

func (c *DiskCache) cachePath(key string) (string, error) {
	// NOTE: avoid giving access to delete arbitrary files on the system
	abs, err := filepath.Abs(path.Join(c.rootDir, key))
	if err != nil {
		return "", fmt.Errorf("getting abs path of '%s': %w", key, err)
	}

	if !strings.HasPrefix(abs, c.rootDir) {
		return "", fmt.Errorf("attempting to delete key outside of root dir '%s': %w", c.rootDir, ErrUnauthorized)
	}

	return path.Join(c.rootDir, key), nil
}

func newCacheWriter(destPath string) (*cacheWriter, error) {
	tmpFile, err := os.CreateTemp("", "seb_*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}

	return &cacheWriter{
		tmpFile:  tmpFile,
		destPath: destPath,
	}, nil
}

type cacheWriter struct {
	tmpFile  *os.File
	destPath string
}

func (cw *cacheWriter) Write(bs []byte) (int, error) {
	n, err := cw.tmpFile.Write(bs)
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

	return nil
}
