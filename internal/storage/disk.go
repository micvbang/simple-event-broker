package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/micvbang/go-helpy/filepathy"
	"github.com/micvbang/go-helpy/uint64y"
	"github.com/micvbang/simple-commit-log/internal/recordbatch"
)

const recordBatchExtension = ".record_batch"

type DiskStorage struct {
	root           string
	nextRecordID   uint64
	recordBatchIDs []uint64
}

func NewDiskStorage(rootDir string, topic string) (*DiskStorage, error) {
	topicDir := filepath.Join(rootDir, topic)
	recordBatchIDs, err := listRecordBatches(topicDir)
	if err != nil {
		return nil, fmt.Errorf("listing record batches: %w", err)
	}

	var nextRecordID uint64
	if len(recordBatchIDs) > 0 {
		newestRecordBatchID := recordBatchIDs[len(recordBatchIDs)-1]
		hdr, err := readRecordBatchHeader(topicDir, newestRecordBatchID)
		if err != nil {
			return nil, fmt.Errorf("reading record batch header: %w", err)
		}
		nextRecordID = newestRecordBatchID + uint64(hdr.NumRecords) + 1
	}

	return &DiskStorage{
		root:           topicDir,
		nextRecordID:   nextRecordID,
		recordBatchIDs: recordBatchIDs,
	}, nil
}

func (ds *DiskStorage) AddRecordBatch(records [][]byte) error {
	err := os.MkdirAll(ds.root, os.ModePerm)
	if err != nil {
		return fmt.Errorf("creating topic dir: %w", err)
	}

	recordBatchID := ds.nextRecordID
	rbPath := recordBatchPath(ds.root, recordBatchID)
	f, err := os.Create(rbPath)
	if err != nil {
		return fmt.Errorf("opening file '%s': %w", rbPath, err)
	}

	err = recordbatch.Write(f, records)
	if err != nil {
		return fmt.Errorf("writing record batch: %w", err)
	}
	ds.recordBatchIDs = append(ds.recordBatchIDs, recordBatchID)
	ds.nextRecordID = recordBatchID + uint64(len(records))

	return nil
}

func (ds *DiskStorage) ReadRecord(recordID uint64) ([]byte, error) {
	if recordID >= ds.nextRecordID {
		return nil, fmt.Errorf("record ID does not exist: %w", ErrOutOfBounds)
	}

	var recordBatchID uint64
	for i := len(ds.recordBatchIDs) - 1; i >= 0; i-- {
		curBatchID := ds.recordBatchIDs[i]
		if curBatchID <= recordID {
			recordBatchID = curBatchID
			break
		}
	}

	rbPath := recordBatchPath(ds.root, recordBatchID)
	f, err := os.Open(rbPath)
	if err != nil {
		return nil, fmt.Errorf("opening record batch '%s': %w", rbPath, err)
	}

	rb, err := recordbatch.Parse(f)
	if err != nil {
		return nil, fmt.Errorf("parsing record batch '%s': %w", rbPath, err)
	}
	return rb.Record(uint32(recordID - recordBatchID))
}

func readRecordBatchHeader(root string, recordBatchID uint64) (recordbatch.Header, error) {
	rbPath := recordBatchPath(root, recordBatchID)
	f, err := os.Open(rbPath)
	if err != nil {
		return recordbatch.Header{}, fmt.Errorf("opening recordBatch '%s': %w", rbPath, err)
	}
	rb, err := recordbatch.Parse(f)
	if err != nil {
		return recordbatch.Header{}, fmt.Errorf("parsing record batch '%s': %w", rbPath, err)
	}

	return rb.Header, nil
}

func recordBatchPath(root string, recordBatchID uint64) string {
	return filepath.Join(root, fmt.Sprintf("%012d%s", recordBatchID, recordBatchExtension))
}

// listRecordBatches returns the record batch identifiers identified
func listRecordBatches(root string) ([]uint64, error) {
	recordIDs := make([]uint64, 0, 128)

	walkConfig := filepathy.WalkConfig{Files: true, Extensions: []string{recordBatchExtension}}
	err := filepathy.Walk(root, walkConfig, func(path string, info os.FileInfo, _ error) error {
		name := info.Name()
		recordIDStr := name[:len(name)-len(recordBatchExtension)]

		recordID, err := uint64y.FromString(recordIDStr)
		if err != nil {
			return err
		}
		recordIDs = append(recordIDs, recordID)
		return nil
	})

	return recordIDs, err
}
