package topic

import (
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/micvbang/go-helpy/uint64y"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/cache"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/recordbatch"
)

type File struct {
	Size int64
	Path string
}

type Storage interface {
	Writer(recordBatchPath string) (io.WriteCloser, error)
	Reader(recordBatchPath string) (io.ReadCloser, error)
	ListFiles(topicName string, extension string) ([]File, error)
}

type Compress interface {
	NewWriter(io.Writer) (io.WriteCloser, error)
	NewReader(io.Reader) (io.ReadCloser, error)
}

type Topic struct {
	log        logger.Logger
	topicName  string
	nextOffset atomic.Uint64

	mu                 sync.Mutex
	recordBatchOffsets []uint64

	backingStorage Storage
	cache          *cache.Cache
	compress       Compress
}

func New(log logger.Logger, backingStorage Storage, topicName string, cache *cache.Cache, compress Compress) (*Topic, error) {
	if cache == nil {
		return nil, fmt.Errorf("cache required")
	}

	recordBatchOffsets, err := listRecordBatchOffsets(backingStorage, topicName)
	if err != nil {
		return nil, fmt.Errorf("listing record batches: %w", err)
	}

	storage := &Topic{
		log:                log.WithField("topic-name", topicName),
		backingStorage:     backingStorage,
		topicName:          topicName,
		recordBatchOffsets: recordBatchOffsets,
		cache:              cache,
		compress:           compress,
	}

	if len(recordBatchOffsets) > 0 {
		newestRecordBatchOffset := recordBatchOffsets[len(recordBatchOffsets)-1]
		parser, err := storage.parseRecordBatch(newestRecordBatchOffset)
		if err != nil {
			return nil, fmt.Errorf("reading record batch header: %w", err)
		}
		storage.nextOffset.Store(newestRecordBatchOffset + uint64(parser.Header.NumRecords))
	}

	return storage, nil
}

// AddRecordBatch writes recordBatch to the topic's backing storage and returns
// the ids of the newly added records in the same order as the records were given.
// NOTE: AddRecordBatch is NOT thread safe. It's up to the caller to ensure that
// this is not called concurrently.
func (s *Topic) AddRecordBatch(recordBatch recordbatch.RecordBatch) ([]uint64, error) {
	recordBatchID := s.nextOffset.Load()

	rbPath := RecordBatchKey(s.topicName, recordBatchID)
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
	// once Close() returns, the data has been committed (stored in backing storage)
	// and can be found
	backingWriter.Close()

	nextOffset := recordBatchID + uint64(len(recordBatch))

	s.log.Infof("wrote %d records (%d bytes) to %s (%s)", len(recordBatch), recordBatch.Size(), rbPath, time.Since(t0))

	offsets := make([]uint64, 0, len(recordBatch))
	for i := recordBatchID; i < nextOffset; i++ {
		offsets = append(offsets, i)
	}

	// once Store() returns, the newly added records are visible in
	// ReadRecord(). NOTE: recordBatchIDs must also have been updated before
	// this is true.
	s.mu.Lock()
	s.recordBatchOffsets = append(s.recordBatchOffsets, recordBatchID)
	s.mu.Unlock()
	s.nextOffset.Store(nextOffset)

	// TODO: it would be nice to remove this from the "fastpath"
	// NOTE: we are intentionally not returning caching errors to caller. It's
	// (semi) fine if the file isn't written to cache since we can retrieve it
	// from backing storage.
	if s.cache != nil {
		cacheWtr, err := s.cache.Writer(rbPath)
		if err != nil {
			s.log.Errorf("creating cache writer to cache (%s): %w", rbPath, err)
			return offsets, nil
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

	return offsets, nil
}

func (s *Topic) ReadRecord(offset uint64) (recordbatch.Record, error) {
	if offset >= s.nextOffset.Load() {
		return nil, fmt.Errorf("offset does not exist: %w", seb.ErrOutOfBounds)
	}

	s.mu.Lock()
	var recordBatchID uint64
	for i := len(s.recordBatchOffsets) - 1; i >= 0; i-- {
		curBatchID := s.recordBatchOffsets[i]
		if curBatchID <= offset {
			recordBatchID = curBatchID
			break
		}
	}
	s.mu.Unlock()

	rb, err := s.parseRecordBatch(recordBatchID)
	if err != nil {
		return nil, fmt.Errorf("parsing record batch: %w", err)
	}

	record, err := rb.Record(uint32(offset - recordBatchID))
	if err != nil {
		return nil, fmt.Errorf("record batch '%s': %w", s.recordBatchPath(recordBatchID), err)
	}
	return record, nil
}

// EndOffset returns the topic's largest offset (most recent record added).
func (s *Topic) EndOffset() uint64 {
	return s.nextOffset.Load()
}

func (s *Topic) parseRecordBatch(recordBatchID uint64) (*recordbatch.Parser, error) {
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

func (s *Topic) recordBatchPath(recordBatchID uint64) string {
	return RecordBatchKey(s.topicName, recordBatchID)
}

const recordBatchExtension = ".record_batch"

func listRecordBatchOffsets(backingStorage Storage, topicName string) ([]uint64, error) {
	files, err := backingStorage.ListFiles(topicName, recordBatchExtension)
	if err != nil {
		return nil, fmt.Errorf("listing files: %w", err)
	}

	offsets := make([]uint64, 0, len(files))
	for _, file := range files {
		fileName := path.Base(file.Path)
		offsetStr := fileName[:len(fileName)-len(recordBatchExtension)]

		offset, err := uint64y.FromString(offsetStr)
		if err != nil {
			return nil, err
		}

		offsets = append(offsets, offset)
	}

	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})

	return offsets, nil
}

// RecordBatchKey returns the symbolic path of the topicName and the recordBatchID.
func RecordBatchKey(topicName string, recordBatchID uint64) string {
	return filepath.Join(topicName, fmt.Sprintf("%012d%s", recordBatchID, recordBatchExtension))
}
