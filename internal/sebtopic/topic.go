package sebtopic

import (
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/go-helpy/uint64y"
	seb "github.com/micvbang/simple-event-broker"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
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
	cache          *sebcache.Cache
	compression    Compress
	OffsetCond     *OffsetCond
}

type Opts struct {
	Compression Compress
}

func New(log logger.Logger, backingStorage Storage, topicName string, cache *sebcache.Cache, optFuncs ...func(*Opts)) (*Topic, error) {
	opts := Opts{
		Compression: Gzip{},
	}
	for _, optFunc := range optFuncs {
		optFunc(&opts)
	}

	recordBatchOffsets, err := listRecordBatchOffsets(backingStorage, topicName)
	if err != nil {
		return nil, fmt.Errorf("listing record batches: %w", err)
	}

	topic := &Topic{
		log:                log.WithField("topic-name", topicName),
		backingStorage:     backingStorage,
		topicName:          topicName,
		recordBatchOffsets: recordBatchOffsets,
		cache:              cache,
		compression:        opts.Compression,
		OffsetCond:         NewOffsetCond(0),
	}

	if len(recordBatchOffsets) > 0 {
		newestRecordBatchOffset := recordBatchOffsets[len(recordBatchOffsets)-1]
		parser, err := topic.parseRecordBatch(newestRecordBatchOffset)
		if err != nil {
			return nil, fmt.Errorf("reading record batch header: %w", err)
		}
		defer parser.Close()

		topic.nextOffset.Store(newestRecordBatchOffset + uint64(parser.Header.NumRecords))
		topic.OffsetCond = NewOffsetCond(newestRecordBatchOffset)
	}

	return topic, nil
}

// AddRecords writes records to the topic's backing storage and returns the ids
// of the newly added records in the same order as the records were given.
//
// NOTE: AddRecords is NOT thread safe. It's up to the caller to ensure that
// this is not called concurrently. This is normally the responsibility of a
// RecordBatcher.
func (s *Topic) AddRecords(records []sebrecords.Record) ([]uint64, error) {
	recordBatchID := s.nextOffset.Load()

	rbPath := RecordBatchKey(s.topicName, recordBatchID)
	backingWriter, err := s.backingStorage.Writer(rbPath)
	if err != nil {
		return nil, fmt.Errorf("opening writer '%s': %w", rbPath, err)
	}

	w := backingWriter
	if s.compression != nil {
		w, err = s.compression.NewWriter(backingWriter)
		if err != nil {
			return nil, fmt.Errorf("creating compression writer: %w", err)
		}
	}

	t0 := time.Now()
	err = sebrecords.Write(w, records)
	if err != nil {
		return nil, fmt.Errorf("writing record batch: %w", err)
	}

	if s.compression != nil {
		w.Close()
	}
	// once Close() returns, the data has been committed and can be retrieved by
	// ReadRecord.
	backingWriter.Close()

	nextOffset := recordBatchID + uint64(len(records))

	recordsSize := 0
	for _, record := range records {
		recordsSize += len(record)
	}
	s.log.Infof("wrote %d records (%s bytes) to %s (%s)", len(records), sizey.FormatBytes(recordsSize), rbPath, time.Since(t0))

	offsets := make([]uint64, 0, len(records))
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

		err = sebrecords.Write(cacheWtr, records)
		if err != nil {
			s.log.Errorf("writing to cache (%s): %w", rbPath, err)
		}

		err = cacheWtr.Close()
		if err != nil {
			s.log.Errorf("closing cached file (%s): %w", rbPath, err)
		}
	}

	// inform potentially waiting consumers that new offsets have been added
	if len(offsets) > 0 {
		s.OffsetCond.Broadcast(offsets[len(offsets)-1])
	}

	return offsets, nil
}

func (s *Topic) ReadRecord(offset uint64) (sebrecords.Record, error) {
	if offset >= s.nextOffset.Load() {
		return nil, fmt.Errorf("offset does not exist: %w", seb.ErrOutOfBounds)
	}

	recordBatchID := s.offsetGetRecordBatchID(offset)

	rb, err := s.parseRecordBatch(recordBatchID)
	if err != nil {
		return nil, fmt.Errorf("parsing record batch: %w", err)
	}
	defer rb.Close()

	record, err := rb.Record(uint32(offset - recordBatchID))
	if err != nil {
		return nil, fmt.Errorf("record batch '%s': %w", s.recordBatchPath(recordBatchID), err)
	}
	return record, nil
}

// ReadRecords returns records starting from startOffset and until either:
// 1) ctx is cancelled
// 2) maxRecords has been reached
// 3) softMaxBytes has been reached
//
// - maxRecords defaults to 10 if 0 is given.
// - softMaxBytes defaults to inifinity if 0 is given;
// - softMaxBytes is "soft" because it will not be honored if it means returning
// zero records; in this case, at least one record will be returned.
//
// NOTE: ReadRecords will always return all of the records that it managed
// to fetch until one of the above conditions were met. This means that the
// returned value should be used even if err is non-nil!
func (s *Topic) ReadRecords(ctx context.Context, offset uint64, maxRecords int, softMaxBytes int) ([]sebrecords.Record, error) {
	if offset >= s.nextOffset.Load() {
		return nil, fmt.Errorf("offset does not exist: %w", seb.ErrOutOfBounds)
	}

	if maxRecords == 0 {
		maxRecords = 10
	}

	records := make([]sebrecords.Record, 0, maxRecords)

	// make a local copy of recordBatchOffsets so that we don't have to hold the
	// lock for the rest of the function.
	s.mu.Lock()
	recordBatchOffsets := make([]uint64, len(s.recordBatchOffsets))
	copy(recordBatchOffsets, s.recordBatchOffsets)
	s.mu.Unlock()

	var (
		batchOffset      uint64
		batchOffsetIndex int
	)
	for batchOffsetIndex = len(recordBatchOffsets) - 1; batchOffsetIndex >= 0; batchOffsetIndex-- {
		curBatchOffset := recordBatchOffsets[batchOffsetIndex]
		if curBatchOffset <= offset {
			batchOffset = curBatchOffset
			break
		}
	}

	rb, err := s.parseRecordBatch(batchOffset)
	if err != nil {
		return nil, fmt.Errorf("parsing record batch: %w", err)
	}

	trackByteSize := softMaxBytes != 0
	recordBatchBytes := 0

	// index of record within the current batch
	batchRecordIndex := uint32(offset - batchOffset)
	for len(records) < maxRecords {
		select {
		case <-ctx.Done():
			return records, ctx.Err()
		default:
		}

		// no more records left in batch -> look for records in next batch
		if batchRecordIndex == rb.Header.NumRecords {
			// close current batch
			rb.Close()

			// move to next batch
			batchOffsetIndex += 1
			if batchOffsetIndex >= len(recordBatchOffsets) {
				// NOTE: this means there's no next batch
				return records, nil
			}
			batchRecordIndex = 0
			batchOffset = recordBatchOffsets[batchOffsetIndex]
			rb, err = s.parseRecordBatch(batchOffset)
			if err != nil {
				return records, fmt.Errorf("parsing record batch: %w", err)
			}
		}

		record, err := rb.Record(batchRecordIndex)
		if err != nil {
			return records, fmt.Errorf("record batch '%s': %w", s.recordBatchPath(batchOffset), err)
		}

		firstRecord := len(records) == 0
		withinByteSize := recordBatchBytes+len(record) <= softMaxBytes

		// Possibilities:
		// 1) we don't care about the size
		// 2) we care about the size but the first record is larger than the
		// given soft max. In order not to potentially block the consumer
		// indefinitely, we return at least one record.
		// 3) we care about the size and it has to be within the soft max
		if !trackByteSize || firstRecord || trackByteSize && withinByteSize {
			recordBatchBytes += len(record)
			records = append(records, record)
			batchRecordIndex += 1
		}

		if trackByteSize && !withinByteSize {
			break
		}

	}
	rb.Close()

	return records, nil
}

// NextOffset returns the topic's next offset (offset of the next record added).
func (s *Topic) NextOffset() uint64 {
	return s.nextOffset.Load()
}

type Metadata struct {
	NextOffset     uint64
	LatestCommitAt time.Time
}

// Metadata returns metadata about the topic
func (s *Topic) Metadata() (Metadata, error) {
	var latestCommitAt time.Time

	nextOffset := s.nextOffset.Load()
	if nextOffset > 0 {
		recordBatchID := s.offsetGetRecordBatchID(nextOffset - 1)
		p, err := s.parseRecordBatch(recordBatchID)
		if err != nil {
			return Metadata{}, fmt.Errorf("parsing record batch: %w", err)
		}

		latestCommitAt = time.UnixMicro(p.Header.UnixEpochUs)
	}

	return Metadata{
		NextOffset:     nextOffset,
		LatestCommitAt: latestCommitAt,
	}, nil
}

func (s *Topic) parseRecordBatch(recordBatchID uint64) (*sebrecords.Parser, error) {
	recordBatchPath := s.recordBatchPath(recordBatchID)

	// NOTE: f is given to sebrecords.Parser, which will own it and be responsible
	// for closing it.
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
		if s.compression != nil {
			r, err = s.compression.NewReader(backingReader)
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

		if s.compression != nil {
			r.Close()
		}

		err = cacheFile.Close()
		if err != nil {
			return nil, fmt.Errorf("closing cacheFile: %w", err)
		}

		err = backingReader.Close()
		if err != nil {
			return nil, fmt.Errorf("closing backing reader: %w", err)
		}

		f, err = s.cache.Reader(recordBatchPath)
		if err != nil {
			return nil, fmt.Errorf("reading from cache just after writing it: %w", err)
		}
	}

	rb, err := sebrecords.Parse(f)
	if err != nil {
		return nil, fmt.Errorf("parsing record batch '%s': %w", recordBatchPath, err)
	}
	return rb, nil
}

func (s *Topic) offsetGetRecordBatchID(offset uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := len(s.recordBatchOffsets) - 1; i >= 0; i-- {
		curBatchID := s.recordBatchOffsets[i]
		if curBatchID <= offset {
			return curBatchID
		}
	}

	return 0
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

func WithCompress(c Compress) func(*Opts) {
	return func(o *Opts) {
		o.Compression = c
	}
}
