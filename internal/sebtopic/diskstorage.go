package sebtopic

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/micvbang/go-helpy/filepathy"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
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

func (ds *DiskStorage) Writer(key string) (io.WriteCloser, error) {
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

func (ds *DiskStorage) Reader(key string) (io.ReadCloser, error) {
	batchPath := ds.rootDirPath(key)

	log := ds.log.WithField("key", key).WithField("path", batchPath)

	log.Debugf("opening file")
	f, err := os.Open(batchPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = errors.Join(err, seb.ErrNotInStorage)
		}

		return nil, fmt.Errorf("opening record batch '%s': %w", batchPath, err)
	}

	return f, nil
}

func (ds *DiskStorage) ListFiles(topicName string, extension string) ([]File, error) {
	log := ds.log.
		WithField("topicName", topicName).
		WithField("extension", extension)

	log.Debugf("listing files")
	t0 := time.Now()

	topicPath := ds.rootDirPath(topicName)

	files := make([]File, 0, 128)
	walkConfig := filepathy.WalkConfig{Files: true, Extensions: []string{extension}}
	err := filepathy.Walk(topicPath, walkConfig, func(path string, info os.FileInfo, _ error) error {
		files = append(files, File{
			Size: info.Size(),
			Path: path,
		})
		return nil
	})

	log.Debugf("found %d files (%s)", len(files), time.Since(t0))

	return files, err
}

func (ds *DiskStorage) rootDirPath(key string) string {
	return filepath.Join(ds.rootDir, key)
}
