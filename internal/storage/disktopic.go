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
	log logger.Logger
}

// NewDiskTopicStorage returns a *TopicStorage that stores its data on local
// disk.
func NewDiskTopicStorage(log logger.Logger) *DiskTopicStorage {
	return &DiskTopicStorage{log: log}
}

func (ds *DiskTopicStorage) Writer(recordBatchPath string) (io.WriteCloser, error) {
	log := ds.log.WithField("recordBatchPath", recordBatchPath)

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

func (ds *DiskTopicStorage) Reader(recordBatchPath string) (io.ReadCloser, error) {
	log := ds.log.WithField("recordBatchPath", recordBatchPath)

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

func (ds *DiskTopicStorage) ListFiles(topicPath string, extension string) ([]File, error) {
	log := ds.log.
		WithField("topicPath", topicPath).
		WithField("extension", extension)

	log.Debugf("listing files")
	t0 := time.Now()

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
