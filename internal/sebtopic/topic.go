package sebtopic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/micvbang/go-helpy"
	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/go-helpy/slicey"
	"github.com/micvbang/go-helpy/stringy"
	"github.com/micvbang/go-helpy/uint64y"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/internal/sebcache"
	"github.com/micvbang/simple-event-broker/internal/seboffsets"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/seberr"
)

type File struct {
	Size int64
	Path string
}

//go:generate mocky -i Storage
type Storage interface {
	Writer(ctx context.Context, recordBatchPath string) (io.WriteCloser, error)
	Reader(ctx context.Context, recordBatchPath string) (io.ReadCloser, error)
	ListFiles(ctx context.Context, topicName string, extension string, startAfter *string) ([]File, error)
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

	recordBatchOffsets, err := ListRecordBatchOffsets(context.Background(), log, backingStorage, topicName)
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
		parser, err := topic.parseRecordBatch(context.Background(), newestRecordBatchOffset)
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
	backingWriter, err := s.backingStorage.Writer(context.Background(), rbPath)
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
	// ReadRecords(). NOTE: recordBatchOffsets must also have been updated before
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
		rb, err := s.parseRecordBatch(ctx, batchOffset)
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
		p, err := s.parseRecordBatch(context.Background(), recordBatchID)
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

func (s *Topic) parseRecordBatch(ctx context.Context, recordBatchID uint64) (*sebrecords.Parser, error) {
	recordBatchPath := s.recordBatchPath(recordBatchID)

	// NOTE: f is given to sebrecords.Parser, which will own it and be responsible
	// for closing it.
	f, err := s.cache.Reader(recordBatchPath)
	if err != nil {
		s.log.Infof("%s not found in cache", recordBatchPath)
	}

	if f == nil { // not found in cache
		backingReader, err := s.backingStorage.Reader(ctx, recordBatchPath)
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

const (
	recordBatchExtension = ".record_batch"
	offsetFileExtension  = ".offsets"
	offsetFilePrefix     = "offsets_"
)

// ListRecordBatchOffsets uses storage.ListFiles to identify the offsets of
// record batches that exist in the topic. It attempts to use .offsets files to
// make this operation faster.
func ListRecordBatchOffsets(ctx context.Context, log logger.Logger, backingStorage Storage, topicName string) ([]uint64, error) {
	log.Debugf("Listing '%s' files in topic '%s'", offsetFileExtension, topicName)
	offsetFiles, err := listOffsetsFiles(ctx, backingStorage, topicName)
	if err != nil {
		return nil, fmt.Errorf("listing offset files: %w", err)
	}

	log.Debugf("Found %d '%s' files", len(offsetFiles), offsetFileExtension)

	var offsets []uint64
	if len(offsetFiles) > 0 {
		file := slicey.Last(offsetFiles)
		rdr, err := backingStorage.Reader(ctx, file.Path)
		if err != nil {
			return nil, fmt.Errorf("reading offsets from '%s': %w", file.Path, err)
		}
		defer rdr.Close()

		offsets, err = seboffsets.Parse(rdr)
		if err != nil {
			return nil, fmt.Errorf("parsing '%s': %w", file.Path, err)
		}
	}

	mostRecentOffset := slicey.Last(offsets)
	var startAfter *string
	if len(offsets) > 0 {
		offsetKey := filepath.Base(RecordBatchKey(topicName, mostRecentOffset))
		startAfter = &offsetKey
	}

	log.Debugf("Listing '%s' files in topic '%s' (start after: %s)", recordBatchExtension, topicName, *stringy.StringOrDefault(startAfter, "[not set]"))

	batchFiles, err := backingStorage.ListFiles(ctx, topicName, recordBatchExtension, startAfter)
	if err != nil {
		return nil, fmt.Errorf("listing files: %w", err)
	}
	log.Debugf("Found %d '%s' files", len(batchFiles), recordBatchExtension)

	for _, batchFile := range batchFiles {
		fileName := path.Base(batchFile.Path)
		offsetStr := fileName[:len(fileName)-len(recordBatchExtension)]

		offset, err := uint64y.FromString(offsetStr)
		if err != nil {
			return nil, err
		}

		offsets = append(offsets, offset)
	}

	slices.Sort(offsets)

	log.Debugf("Returning %d offsets, most recent one is %d", len(offsets), slicey.Last(offsets))

	return offsets, nil
}

// WriteRecordBatchOffsets writes the given offsets to the next .offsets file.
// This is used as a caching mechanism that allows us to avoid calling S3's LIST
// OBJECTS endpoint excessively for large topics.
//
// NOTE: THIS OPERATION IS NOT SAFE TO RUN CONCURRENTLY! Not within the same
// program, but also not concurrently with another instance of the program.
func WriteRecordBatchOffsets(ctx context.Context, log logger.Logger, backingStorage Storage, topicName string, offsets []uint64) (err error) {
	offsetFilePaths, err := listOffsetsFiles(ctx, backingStorage, topicName)
	if err != nil {
		return fmt.Errorf("listing offset files: %w", err)
	}

	numOffsetFiles := uint64(len(offsetFilePaths))

	// assert integrity of offset file naming
	{
		for i, offsetFilePath := range offsetFilePaths {
			expectedPath := OffsetsFileKey(topicName, uint64(i))
			if expectedPath != offsetFilePath.Path {
				msg := fmt.Sprintf("expected offset file path '%s', got '%s'", expectedPath, offsetFilePath.Path)
				log.Errorf(msg)
				panic(msg)
			}
		}
	}

	offsetFilePath := OffsetsFileKey(topicName, numOffsetFiles)

	wtr, err := backingStorage.Writer(ctx, offsetFilePath)
	if err != nil {
		return fmt.Errorf("opening writer for '%s': %w", offsetFilePath, err)
	}
	defer func() {
		// NOTE: we do not wish to ignore the potential error returned by
		// closing the writer
		wtrErr := wtr.Close()
		if wtrErr != nil {
			errors.Join(err, fmt.Errorf("closing writer for %s: %w", offsetFilePath, wtrErr))
		}
	}()

	err = seboffsets.Write(wtr, offsets)
	if err != nil {
		return fmt.Errorf("writing offsets to '%s': %w", offsetFilePath, err)
	}

	return nil
}

func listOffsetsFiles(ctx context.Context, backingStorage Storage, topicName string) ([]File, error) {
	return backingStorage.ListFiles(ctx, topicName, offsetFileExtension, helpy.Pointer(offsetFilePrefix))
}

// offsetFileIDs are zero-indexed, i.e. the first one is number 0.
func OffsetsFileKey(topicName string, offsetFileID uint64) string {
	return filepath.Join(topicName, fmt.Sprintf("%s%012d%s", offsetFilePrefix, offsetFileID, offsetFileExtension))
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
