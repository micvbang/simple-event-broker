package storage

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

// MemoryTopicStorage is an in-memory backing storage that can be used in Topic.
// It is mostly useful for testing.
type MemoryTopicStorage struct {
	mu      sync.Mutex
	storage map[string]*bytes.Buffer
}

func NewMemoryTopicStorage(log logger.Logger) *MemoryTopicStorage {
	return &MemoryTopicStorage{
		storage: make(map[string]*bytes.Buffer, 64),
	}
}

func (ms *MemoryTopicStorage) Writer(recordBatchPath string) (io.WriteCloser, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	buf := bytes.NewBuffer(nil)
	ms.storage[recordBatchPath] = buf

	return NopWriteCloser(buf), nil
}

func (ms *MemoryTopicStorage) Reader(recordBatchPath string) (io.ReadCloser, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	buf, ok := ms.storage[recordBatchPath]
	if !ok {
		return nil, ErrNotInStorage
	}

	return io.NopCloser(buf), nil
}

func (ms *MemoryTopicStorage) ListFiles(topicName string, extension string) ([]File, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	files := make([]File, 0, 128)

	topicPrefix := fmt.Sprintf("%s/", topicName)
	for key, buf := range ms.storage {
		if strings.HasPrefix(key, topicPrefix) {
			files = append(files, File{
				Size: int64(buf.Len()),
				Path: key,
			})
		}
	}

	return files, nil
}
