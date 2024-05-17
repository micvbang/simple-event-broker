package cache

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
	seb "github.com/micvbang/simple-event-broker"
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

	// tempDir must point to a directory within rootDir that is part of the same
	// file system as we're writing our cache files to.  The reason is that
	// we're using os.Rename() to move temporary files atomically (for file
	// systems that support this) into the cache. A requirement of this being
	// possible to do atomically, is that the file being moved (renamed) is
	// within the same file system both before and after the move.
	tempDir string
}

func NewDiskStorage(log logger.Logger, rootDir string) (*DiskCache, error) {
	if !strings.HasSuffix(rootDir, "/") {
		rootDir += "/"
	}

	tempDir := filepath.Join(rootDir, "_tmpdir")
	err := os.MkdirAll(tempDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("creating temp dir '%s': %w", tempDir, err)
	}

	return &DiskCache{
		log:     log,
		rootDir: rootDir,
		tempDir: tempDir,
	}, nil
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
	return newCacheWriter(c.tempDir, cachePath)
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
		return nil, errors.Join(seb.ErrNotInCache, fmt.Errorf("opening record batch '%s': %w", key, err))
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
		err := fmt.Errorf("attempting to delete key outside of root dir '%s': %w", c.rootDir, seb.ErrUnauthorized)
		c.log.
			WithField("key", key).
			WithField("abs", abs).
			WithField("root-dir", c.rootDir).
			Errorf(err.Error())

		return "", err
	}

	return path.Join(c.rootDir, key), nil
}

func newCacheWriter(tempDir string, destPath string) (*cacheWriter, error) {
	tmpFile, err := os.CreateTemp(tempDir, "seb_*")
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
	return cw.tmpFile.Write(bs)
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

	// NOTE: os.Rename can only provide atomicity when renaming files within the
	// same file system, so we require that tmpFile is written to the same file
	// system as destPath.
	err = os.Rename(cw.tmpFile.Name(), cw.destPath)
	if err != nil {
		return fmt.Errorf("moving %s to %s: %w", cw.tmpFile.Name(), cw.destPath, err)
	}

	return nil
}
