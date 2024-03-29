package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/micvbang/go-helpy/filepathy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

const recordBatchExtension = ".record_batch"

type DiskStorage struct{}

func NewDiskStorage(log logger.Logger, rootDir string, topic string) (*Storage, error) {
	return NewStorage(log, DiskStorage{}, rootDir, topic)
}

func (DiskStorage) Writer(recordBatchPath string) (io.WriteCloser, error) {
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

func (DiskStorage) Reader(recordBatchPath string) (io.ReadSeekCloser, error) {
	f, err := os.Open(recordBatchPath)
	if err != nil {
		return nil, fmt.Errorf("opening record batch '%s': %w", recordBatchPath, err)
	}

	return f, nil
}

func (DiskStorage) ListFiles(topicPath string, extension string) ([]File, error) {
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
