package storage

import (
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"

	"github.com/micvbang/go-helpy/uint64y"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
)

type File struct {
	Size int64
	Path string
}

type BackingStorage interface {
	Writer(recordBatchPath string) (io.WriteCloser, error)
	Reader(recordBatchPath string) (io.ReadSeekCloser, error)
	ListFiles(topicPath string, extension string) ([]File, error)
}

type TopicStorage struct {
	log            logger.Logger
	topicPath      string
	nextRecordID   uint64
	recordBatchIDs []uint64

	backingStorage BackingStorage
}

func NewTopicStorage(log logger.Logger, backingStorage BackingStorage, rootDir string, topic string) (*TopicStorage, error) {
	topicPath := filepath.Join(rootDir, topic)

	recordBatchIDs, err := listRecordBatchIDs(backingStorage, topicPath)
	if err != nil {
		return nil, fmt.Errorf("listing record batches: %w", err)
	}

	storage := &TopicStorage{
		log:            log,
		backingStorage: backingStorage,
		topicPath:      topicPath,
		recordBatchIDs: recordBatchIDs,
	}

	if len(recordBatchIDs) > 0 {
		newestRecordBatchID := recordBatchIDs[len(recordBatchIDs)-1]
		hdr, err := readRecordBatchHeader(backingStorage, topicPath, newestRecordBatchID)
		if err != nil {
			return nil, fmt.Errorf("reading record batch header: %w", err)
		}
		storage.nextRecordID = newestRecordBatchID + uint64(hdr.NumRecords)
	}

	return storage, nil
}

func (s *TopicStorage) AddRecordBatch(recordBatch recordbatch.RecordBatch) error {
	recordBatchID := s.nextRecordID

	rbPath := recordBatchPath(s.topicPath, recordBatchID)
	f, err := s.backingStorage.Writer(rbPath)
	if err != nil {
		return fmt.Errorf("opening writer '%s': %w", rbPath, err)
	}
	defer f.Close()

	err = recordbatch.Write(f, recordBatch)
	if err != nil {
		return fmt.Errorf("writing record batch: %w", err)
	}
	s.recordBatchIDs = append(s.recordBatchIDs, recordBatchID)
	s.nextRecordID = recordBatchID + uint64(len(recordBatch))

	return nil
}

func (s *TopicStorage) ReadRecord(recordID uint64) (recordbatch.Record, error) {
	if recordID >= s.nextRecordID {
		return nil, fmt.Errorf("record ID does not exist: %w", ErrOutOfBounds)
	}

	var recordBatchID uint64
	for i := len(s.recordBatchIDs) - 1; i >= 0; i-- {
		curBatchID := s.recordBatchIDs[i]
		if curBatchID <= recordID {
			recordBatchID = curBatchID
			break
		}
	}

	rbPath := recordBatchPath(s.topicPath, recordBatchID)
	f, err := s.backingStorage.Reader(rbPath)
	if err != nil {
		return nil, fmt.Errorf("opening reader '%s': %w", rbPath, err)
	}

	rb, err := recordbatch.Parse(f)
	if err != nil {
		return nil, fmt.Errorf("parsing record batch '%s': %w", rbPath, err)
	}

	record, err := rb.Record(uint32(recordID - recordBatchID))
	if err != nil {
		return nil, fmt.Errorf("record batch '%s': %w", rbPath, err)
	}
	return record, nil
}
func readRecordBatchHeader(backingStorage BackingStorage, topicPath string, recordBatchID uint64) (recordbatch.Header, error) {
	rbPath := recordBatchPath(topicPath, recordBatchID)
	f, err := backingStorage.Reader(rbPath)
	if err != nil {
		return recordbatch.Header{}, fmt.Errorf("opening recordBatch '%s': %w", rbPath, err)
	}

	rb, err := recordbatch.Parse(f)
	if err != nil {
		return recordbatch.Header{}, fmt.Errorf("parsing record batch '%s': %w", rbPath, err)
	}

	return rb.Header, nil
}

const recordBatchExtension = ".record_batch"

func listRecordBatchIDs(backingStorage BackingStorage, topicPath string) ([]uint64, error) {
	files, err := backingStorage.ListFiles(topicPath, recordBatchExtension)
	if err != nil {
		return nil, fmt.Errorf("listing files: %w", err)
	}

	recordIDs := make([]uint64, 0, len(files))
	for _, file := range files {
		fileName := path.Base(file.Path)
		recordIDStr := fileName[:len(fileName)-len(recordBatchExtension)]

		recordID, err := uint64y.FromString(recordIDStr)
		if err != nil {
			return nil, err
		}

		recordIDs = append(recordIDs, recordID)
	}

	sort.Slice(recordIDs, func(i, j int) bool {
		return recordIDs[i] < recordIDs[j]
	})

	return recordIDs, nil
}

func recordBatchPath(topicPath string, recordBatchID uint64) string {
	return filepath.Join(topicPath, fmt.Sprintf("%012d%s", recordBatchID, recordBatchExtension))
}
