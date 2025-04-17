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
	"github.com/micvbang/go-helpy/slicey"
	"github.com/micvbang/go-helpy/uint64y"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/seberr"
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

		nextOffset := newestRecordBatchOffset + uint64(parser.Header.NumRecords)
		topic.nextOffset.Store(nextOffset)
		topic.OffsetCond = NewOffsetCond(nextOffset - 1)
	}

	return topic, nil
}

// AddRecords writes records to the topic's backing storage and returns the ids
// of the newly added records in the same order as the records were given.
//
// NOTE: AddRecords is NOT thread safe. It's up to the caller to ensure that
// this is not called concurrently. This is normally the responsibility of a
// RecordBatcher.
func (s *Topic) AddRecords(batch sebrecords.Batch) ([]uint64, error) {
	s.log.Debugf("Adding %d records (%d bytes)", batch.Len(), len(batch.Data))
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
	err = sebrecords.Write(w, batch)
	if err != nil {
		return nil, fmt.Errorf("writing record batch: %w", err)
	}

	if s.compression != nil {
		err = w.Close()
		if err != nil {
			return nil, fmt.Errorf("closing compression writer: %w", err)
		}
	}
	// once Close() returns, the data has been committed and can be retrieved by
	// ReadRecord.
	err = backingWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("closing backing writer: %w", err)
	}

	s.log.Infof("wrote %d records (%s bytes) to %s (%s)", batch.Len(), sizey.FormatBytes(len(batch.Data)), rbPath, time.Since(t0))

	nextOffset := recordBatchID + uint64(batch.Len())
	offsets := make([]uint64, 0, batch.Len())
	for i := recordBatchID; i < nextOffset; i++ {
		offsets = append(offsets, i)
	}

	// once Store() returns, the newly added records are visible in
	// ReadRecords(). NOTE: recordBatchIDs must also have been updated before
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

		err = sebrecords.Write(cacheWtr, batch)
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
		s.OffsetCond.Broadcast(slicey.Last(offsets))
	}

	return offsets, nil
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
func (s *Topic) ReadRecords(ctx context.Context, batch *sebrecords.Batch, offset uint64, maxRecords int, softMaxBytes int) error {
	if offset >= s.nextOffset.Load() {
		return fmt.Errorf("offset does not exist: %w", seberr.ErrOutOfBounds)
	}

	if maxRecords == 0 {
		maxRecords = 10
	}

	// make a local copy of recordBatchOffsets so that we don't have to hold the
	// lock for the rest of the function.
	s.mu.Lock()
	recordBatchOffsets := make([]uint64, len(s.recordBatchOffsets))
	copy(recordBatchOffsets, s.recordBatchOffsets)
	s.mu.Unlock()

	// find the batch that offset is located in
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

	trackByteSize := softMaxBytes != 0
	recordBatchBytes := uint32(0)
	batchRecordIndex := uint32(offset - batchOffset)
	firstRecord := true

	moreRecords := func() bool { return batch.Len() < maxRecords }
	moreBytes := func() bool { return (!trackByteSize || recordBatchBytes < uint32(softMaxBytes)) }
	moreBatches := func() bool { return batchOffsetIndex < len(recordBatchOffsets) }

	for moreRecords() && moreBytes() && moreBatches() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchOffset = recordBatchOffsets[batchOffsetIndex]
		rb, err := s.parseRecordBatch(batchOffset)
		if err != nil {
			return fmt.Errorf("parsing record batch: %w", err)
		}

		batchMaxRecords := min(uint32(maxRecords-batch.Len()), rb.Header.NumRecords-batchRecordIndex)
		numRecords := batchMaxRecords
		if trackByteSize {
			numRecords = 0

			for _, recordSize := range rb.RecordSizes[batchRecordIndex : batchRecordIndex+batchMaxRecords] {
				if !firstRecord && recordBatchBytes+recordSize > uint32(softMaxBytes) {
					break
				}

				numRecords += 1
				recordBatchBytes += recordSize
				firstRecord = false
			}
		}

		// we read enough records to satisfy the request
		if numRecords == 0 {
			break
		}

		err = rb.Records(batch, batchRecordIndex, batchRecordIndex+numRecords)
		if err != nil {
			return fmt.Errorf("record batch '%s': %w", s.recordBatchPath(batchOffset), err)
		}

		// no more relevant records in batch -> prepare to check next batch
		rb.Close()
		batchOffsetIndex += 1
		batchRecordIndex = 0
	}

	return nil
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
