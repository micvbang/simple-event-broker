package sebtopic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/micvbang/go-helpy/filepathy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-event-broker/seberr"
)

type DiskStorage struct {
	log     logger.Logger
	rootDir string
}

// NewDiskStorage returns a *DiskStorage that stores its data in rootDir on
// local disk.
func NewDiskStorage(log logger.Logger, rootDir string) *DiskStorage {
	return &DiskStorage{
		log:     log,
		rootDir: rootDir,
	}
}

func (ds *DiskStorage) Writer(_ context.Context, key string) (io.WriteCloser, error) {
	batchPath := ds.rootDirPath(key)

	log := ds.log.WithField("key", key).WithField("path", batchPath)

	log.Debugf("creating dirs")
	err := mkdirAllDurable(filepath.Dir(batchPath), syncDirectory)
	if err != nil {
		return nil, fmt.Errorf("creating topic dir: %w", err)
	}

	log.Debugf("creating file")
	f, err := os.CreateTemp(filepath.Dir(batchPath), ".seb-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("opening file '%s': %w", batchPath, err)
	}

	return &diskBatchWriter{
		file:     f,
		destPath: batchPath,
	}, nil
}

// Persist each directory's entry in its parent before creating children.
// Sync existing entries too, so retrying a failed sync cannot skip durability.
func mkdirAllDurable(dir string, syncDir func(string) error) error {
	dir = filepath.Clean(dir)
	parent := filepath.Dir(dir)
	if parent == dir {
		return nil
	}

	if err := mkdirAllDurable(parent, syncDir); err != nil {
		return err
	}

	err := os.Mkdir(dir, os.ModePerm)
	if errors.Is(err, os.ErrExist) {
		info, statErr := os.Stat(dir)
		if statErr != nil {
			return statErr
		}

		if !info.IsDir() {
			return err
		}
	} else if err != nil {
		return err
	}

	return syncDir(parent)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening directory %q: %w", path, err)
	}

	if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
		return fmt.Errorf("syncing directory %q: %w", path, err)
	}

	return nil
}

// Stage in the destination directory so a successful rename is atomic and
// uncommitted files never appear in batch or offset-file listings.
type diskBatchWriter struct {
	file      *os.File
	destPath  string
	writeErr  error
	finished  bool
	finishErr error
}

func (w *diskBatchWriter) Write(p []byte) (int, error) {
	if w.finished {
		return 0, io.ErrClosedPipe
	}

	if w.writeErr != nil {
		return 0, w.writeErr
	}

	n, err := w.file.Write(p)
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}

	w.writeErr = err

	return n, err
}

func (w *diskBatchWriter) Abort() error {
	if w.finished {
		return w.finishErr
	}

	w.finished = true
	w.finishErr = errors.Join(w.file.Close(), os.Remove(w.file.Name()))

	return w.finishErr
}

func (w *diskBatchWriter) Close() (err error) {
	if w.finished {
		return w.finishErr
	}
	defer func() { w.finishErr = err }()

	if w.writeErr != nil {
		return errors.Join(w.writeErr, w.Abort())
	}

	if err = w.file.Sync(); err != nil {
		return errors.Join(fmt.Errorf("syncing staged batch: %w", err), w.Abort())
	}

	w.finished = true
	if err = w.file.Close(); err != nil {
		return errors.Join(err, os.Remove(w.file.Name()))
	}

	if err = os.Rename(w.file.Name(), w.destPath); err != nil {
		return errors.Join(fmt.Errorf("publishing staged batch: %w", err), os.Remove(w.file.Name()))
	}

	// Persist the rename before acknowledging the commit.
	return syncDirectory(filepath.Dir(w.destPath))
}

func (ds *DiskStorage) Reader(_ context.Context, key string) (io.ReadCloser, error) {
	batchPath := ds.rootDirPath(key)

	log := ds.log.WithField("key", key).WithField("path", batchPath)

	log.Debugf("opening file")
	f, err := os.Open(batchPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = errors.Join(err, seberr.ErrNotInStorage)
		}

		return nil, fmt.Errorf("opening record batch '%s': %w", batchPath, err)
	}

	return f, nil
}

func (ds *DiskStorage) ListFiles(_ context.Context, topicName string, extension string, startAfter *string) ([]File, error) {
	log := ds.log.
		WithField("topicName", topicName).
		WithField("extension", extension)

	log.Debugf("listing files")
	t0 := time.Now()

	rootDirPath := ds.rootDirPath("")
	topicPath := ds.rootDirPath(topicName)

	files := make([]File, 0, 128)
	walkConfig := filepathy.WalkConfig{Files: true, Extensions: []string{extension}}
	err := filepathy.Walk(topicPath, walkConfig, func(path string, info os.FileInfo, _ error) error {
		// NOTE: Base could return "." or "/"
		if startAfter != nil && filepath.Base(path) <= *startAfter {
			return nil
		}

		relPath, err := filepath.Rel(rootDirPath, path)
		if err != nil {
			return fmt.Errorf("getting relative path for '%s': %w", path, err)
		}
		files = append(files, File{
			Size: info.Size(),
			Path: relPath,
		})
		return nil
	})

	log.Debugf("found %d files (%s)", len(files), time.Since(t0))

	return files, err
}

func (ds *DiskStorage) rootDirPath(key string) string {
	return filepath.Join(ds.rootDir, key)
}
