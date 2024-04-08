package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
)

// DiskCache is used to cache RecordBatches on the local disk.
type DiskCache struct {
	rootdir string
}

func NewCache(rootDir string) *DiskCache {
	return &DiskCache{rootdir: rootDir}
}

// TODO: somehow to track how much disk space is being used
// TODO: somehow to throw away "old" (no longer needed) data

func (c *DiskCache) Writer(recordBatchPath string) (io.WriteCloser, error) {
	tmpFile, err := os.CreateTemp("", "seb_*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}

	return &cacheWriter{
		tmpFile:  tmpFile,
		destPath: c.cachePath(recordBatchPath),
	}, nil
}

func (c *DiskCache) Reader(recordBatchPath string) (io.ReadSeekCloser, error) {
	f, err := os.Open(c.cachePath(recordBatchPath))
	if err != nil {
		return nil, errors.Join(ErrNotInCache, fmt.Errorf("opening record batch '%s': %w", recordBatchPath, err))
	}

	return f, nil
}

func (c *DiskCache) cachePath(recordBatchPath string) string {
	return path.Join(c.rootdir, recordBatchPath)
}

type cacheWriter struct {
	tmpFile  *os.File
	destPath string
}

func (cw cacheWriter) Write(bs []byte) (int, error) {
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

	err = os.Rename(cw.tmpFile.Name(), cw.destPath)
	if err != nil {
		return fmt.Errorf("moving %s to %s: %w", cw.tmpFile.Name(), cw.destPath, err)
	}

	return nil
}
