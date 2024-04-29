package storage

import (
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"time"

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
	Reader(recordBatchPath string) (io.ReadCloser, error)
	ListFiles(topicPath string, extension string) ([]File, error)
}

type Compress interface {
	NewWriter(io.Writer) (io.WriteCloser, error)
	NewReader(io.Reader) (io.ReadCloser, error)
}

type TopicStorage struct {
	log            logger.Logger
	topicPath      string
	nextRecordID   uint64
	recordBatchIDs []uint64

	backingStorage BackingStorage
	cache          *Cache
	compress       Compress
}

func NewTopicStorage(log logger.Logger, backingStorage BackingStorage, rootDir string, topicName string, cache *Cache, compress Compress) (*TopicStorage, error) {
	if cache == nil {
		return nil, fmt.Errorf("cache required")
	}

	topicPath := filepath.Join(rootDir, topicName)

	recordBatchIDs, err := listRecordBatchIDs(backingStorage, topicPath)
	if err != nil {
		return nil, fmt.Errorf("listing record batches: %w", err)
	}

	storage := &TopicStorage{
		log:            log.WithField("topic-name", topicName),
		backingStorage: backingStorage,
		topicPath:      topicPath,
		recordBatchIDs: recordBatchIDs,
		cache:          cache,
		compress:       compress,
	}

	if len(recordBatchIDs) > 0 {
		newestRecordBatchID := recordBatchIDs[len(recordBatchIDs)-1]
		parser, err := storage.parseRecordBatch(newestRecordBatchID)
		if err != nil {
			return nil, fmt.Errorf("reading record batch header: %w", err)
		}
		storage.nextRecordID = newestRecordBatchID + uint64(parser.Header.NumRecords)
	}

	return storage, nil
}

func (s *TopicStorage) AddRecordBatch(recordBatch recordbatch.RecordBatch) ([]uint64, error) {
	recordBatchID := s.nextRecordID

	rbPath := RecordBatchPath(s.topicPath, recordBatchID)
	backingWriter, err := s.backingStorage.Writer(rbPath)
	if err != nil {
		return nil, fmt.Errorf("opening writer '%s': %w", rbPath, err)
	}

	w := backingWriter
	if s.compress != nil {
		w, err = s.compress.NewWriter(backingWriter)
		if err != nil {
			return nil, fmt.Errorf("creating compression writer: %w", err)
		}
	}

	t0 := time.Now()
	err = recordbatch.Write(w, recordBatch)
	if err != nil {
		return nil, fmt.Errorf("writing record batch: %w", err)
	}

	if s.compress != nil {
		w.Close()
	}
	backingWriter.Close()

	s.log.Infof("wrote %d records (%d bytes) to %s (%s)", len(recordBatch), recordBatch.Size(), rbPath, time.Since(t0))

	recordIDs := make([]uint64, 0, len(recordBatch))
	nextRecordID := recordBatchID + uint64(len(recordBatch))
	for i := recordBatchID; i < nextRecordID; i++ {
		recordIDs = append(recordIDs, i)
	}

	s.recordBatchIDs = append(s.recordBatchIDs, recordBatchID)
	s.nextRecordID = nextRecordID

	// TODO: it would be nice to remove this from the "fastpath"
	// NOTE: we are intentionally not returning caching errors to caller. It's
	// (semi) fine if the file isn't written to cache since we can retrieve it
	// from backing storage.
	if s.cache != nil {
		cacheWtr, err := s.cache.Writer(rbPath)
		if err != nil {
			s.log.Errorf("creating cache writer to cache (%s): %w", rbPath, err)
			return recordIDs, nil
		}

		err = recordbatch.Write(cacheWtr, recordBatch)
		if err != nil {
			s.log.Errorf("writing to cache (%s): %w", rbPath, err)
		}

		err = cacheWtr.Close()
		if err != nil {
			s.log.Errorf("closing cached file (%s): %w", rbPath, err)
		}
	}

	return recordIDs, nil
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

	rb, err := s.parseRecordBatch(recordBatchID)
	if err != nil {
		return nil, fmt.Errorf("parsing record batch: %w", err)
	}

	record, err := rb.Record(uint32(recordID - recordBatchID))
	if err != nil {
		return nil, fmt.Errorf("record batch '%s': %w", s.recordBatchPath(recordBatchID), err)
	}
	return record, nil
}

func (s *TopicStorage) parseRecordBatch(recordBatchID uint64) (*recordbatch.Parser, error) {
	recordBatchPath := s.recordBatchPath(recordBatchID)
	f, err := s.cache.Reader(recordBatchPath)
	if err != nil {
		s.log.Infof("%s not found in cache", recordBatchPath)
	}

	if f == nil { // not found in cache
		backingReader, err := s.backingStorage.Reader(recordBatchPath)
		if err != nil {
			return nil, fmt.Errorf("opening reader '%s': %w", recordBatchPath, err)
		}

		r := backingReader
		if s.compress != nil {
			r, err = s.compress.NewReader(backingReader)
			if err != nil {
				return nil, fmt.Errorf("creating compression reader: %w", err)
			}
		}

		// write to cache
		cacheFile, err := s.cache.Writer(recordBatchPath)
		if err != nil {
			return nil, fmt.Errorf("writing backing storage result to cache: %w", err)
		}
		_, err = io.Copy(cacheFile, r)
		if err != nil {
			return nil, fmt.Errorf("copying backing storage result to cache: %w", err)
		}

		if s.compress != nil {
			r.Close()
		}

		cacheFile.Close()
		backingReader.Close()

		f, err = s.cache.Reader(recordBatchPath)
		if err != nil {
			return nil, fmt.Errorf("reading from cache just after writing it: %w", err)
		}
	}

	rb, err := recordbatch.Parse(f)
	if err != nil {
		return nil, fmt.Errorf("parsing record batch '%s': %w", recordBatchPath, err)
	}
	return rb, nil
}

func (s *TopicStorage) recordBatchPath(recordBatchID uint64) string {
	return RecordBatchPath(s.topicPath, recordBatchID)
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

// RecordBatchPath returns the symbolic path of the topicName and the recordBatchID.
func RecordBatchPath(topicPath string, recordBatchID uint64) string {
	return filepath.Join(topicPath, fmt.Sprintf("%012d%s", recordBatchID, recordBatchExtension))
}
