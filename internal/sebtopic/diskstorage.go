package sebtopic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/micvbang/go-helpy/filepathy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/seberr"
)

type DiskStorage struct {
	log     logger.Logger
	rootDir string
}

// NewDiskStorage returns a *DiskStorage that stores its data in rootDir on
// local disk.
func NewDiskStorage(log logger.Logger, rootDir string) *DiskStorage {
	return &DiskStorage{
		log:     log,
		rootDir: rootDir,
	}
}

func (ds *DiskStorage) Writer(_ context.Context, key string) (io.WriteCloser, error) {
	batchPath := ds.rootDirPath(key)

	log := ds.log.WithField("key", key).WithField("path", batchPath)

	log.Debugf("creating dirs")
	err := os.MkdirAll(filepath.Dir(batchPath), os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("creating topic dir: %w", err)
	}

	log.Debugf("creating file")
	f, err := os.Create(batchPath)
	if err != nil {
		return nil, fmt.Errorf("opening file '%s': %w", batchPath, err)
	}

	return f, nil
}

func (ds *DiskStorage) Reader(_ context.Context, key string) (io.ReadCloser, error) {
	batchPath := ds.rootDirPath(key)

	log := ds.log.WithField("key", key).WithField("path", batchPath)

	log.Debugf("opening file")
	f, err := os.Open(batchPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = errors.Join(err, seberr.ErrNotInStorage)
		}

		return nil, fmt.Errorf("opening record batch '%s': %w", batchPath, err)
	}

	return f, nil
}

func (ds *DiskStorage) ListFiles(_ context.Context, topicName string, extension string, startAfter *string) ([]File, error) {
	log := ds.log.
		WithField("topicName", topicName).
		WithField("extension", extension)

	log.Debugf("listing files")
	t0 := time.Now()

	rootDirPath := ds.rootDirPath("")
	topicPath := ds.rootDirPath(topicName)

	files := make([]File, 0, 128)
	walkConfig := filepathy.WalkConfig{Files: true, Extensions: []string{extension}}
	err := filepathy.Walk(topicPath, walkConfig, func(path string, info os.FileInfo, _ error) error {
		// NOTE: Base could return "." or "/"
		if startAfter != nil && filepath.Base(path) <= *startAfter {
			return nil
		}

		relPath, err := filepath.Rel(rootDirPath, path)
		if err != nil {
			return fmt.Errorf("getting relative path for '%s': %w", path, err)
		}
		files = append(files, File{
			Size: info.Size(),
			Path: relPath,
		})
		return nil
	})

	log.Debugf("found %d files (%s)", len(files), time.Since(t0))

	return files, err
}

func (ds *DiskStorage) rootDirPath(key string) string {
	return filepath.Join(ds.rootDir, key)
}
