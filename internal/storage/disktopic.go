package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/micvbang/go-helpy/filepathy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

type DiskTopicStorage struct{}

// NewDiskTopicStorage returns a *TopicStorage that stores its data on local
// disk.
func NewDiskTopicStorage(log logger.Logger, rootDir string, topic string, cache *DiskCache) (*TopicStorage, error) {
	return NewTopicStorage(log, DiskTopicStorage{}, rootDir, topic, cache)
}

func (DiskTopicStorage) Writer(recordBatchPath string) (io.WriteCloser, error) {
	err := os.MkdirAll(filepath.Dir(recordBatchPath), os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("creating topic dir: %w", err)
	}

	f, err := os.Create(recordBatchPath)
	if err != nil {
		return nil, fmt.Errorf("opening file '%s': %w", recordBatchPath, err)
	}

	return f, nil
}

func (DiskTopicStorage) Reader(recordBatchPath string) (io.ReadCloser, error) {
	f, err := os.Open(recordBatchPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = errors.Join(err, ErrNotInStorage)
		}

		return nil, fmt.Errorf("opening record batch '%s': %w", recordBatchPath, err)
	}

	return f, nil
}

func (DiskTopicStorage) ListFiles(topicPath string, extension string) ([]File, error) {
	files := make([]File, 0, 128)

	walkConfig := filepathy.WalkConfig{Files: true, Extensions: []string{extension}}
	err := filepathy.Walk(topicPath, walkConfig, func(path string, info os.FileInfo, _ error) error {
		files = append(files, File{
			Size: info.Size(),
			Path: path,
		})
		return nil
	})

	return files, err
}
