package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/micvbang/go-helpy/filepathy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

type DiskTopicStorage struct {
	log     logger.Logger
	rootDir string
}

// NewDiskTopicStorage returns a *TopicStorage that stores its data in rootDir
// on local disk.
func NewDiskTopicStorage(log logger.Logger, rootDir string) *DiskTopicStorage {
	return &DiskTopicStorage{
		log:     log,
		rootDir: rootDir,
	}
}

func (ds *DiskTopicStorage) Writer(recordBatchKey string) (io.WriteCloser, error) {
	log := ds.log.WithField("recordBatchKey", recordBatchKey)

	recordBatchPath := ds.recordBatchPath(recordBatchKey)
	log.Debugf("creating dirs")
	err := os.MkdirAll(filepath.Dir(recordBatchPath), os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("creating topic dir: %w", err)
	}

	log.Debugf("creating file")
	f, err := os.Create(recordBatchPath)
	if err != nil {
		return nil, fmt.Errorf("opening file '%s': %w", recordBatchPath, err)
	}

	return f, nil
}

func (ds *DiskTopicStorage) Reader(recordBatchKey string) (io.ReadCloser, error) {
	log := ds.log.WithField("recordBatchName", recordBatchKey)

	recordBatchPath := ds.recordBatchPath(recordBatchKey)

	log.Debugf("opening file")
	f, err := os.Open(recordBatchPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = errors.Join(err, ErrNotInStorage)
		}

		return nil, fmt.Errorf("opening record batch '%s': %w", recordBatchPath, err)
	}

	return f, nil
}

func (ds *DiskTopicStorage) ListFiles(topicName string, extension string) ([]File, error) {
	log := ds.log.
		WithField("topicName", topicName).
		WithField("extension", extension)

	log.Debugf("listing files")
	t0 := time.Now()

	topicPath := ds.recordBatchPath(topicName)

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

func (ds *DiskTopicStorage) recordBatchPath(recordBatchKey string) string {
	return filepath.Join(ds.rootDir, recordBatchKey)
}
