package sebtopic

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestDurableDirectoriesSyncParentsBeforeChildren(t *testing.T) {
	root := t.TempDir()
	storage := filepath.Join(root, "storage")
	topic := filepath.Join(storage, "topic")
	var synced []string
	err := mkdirAllDurable(topic, func(dir string) error {
		synced = append(synced, dir)
		if dir == root {
			if _, err := os.Stat(topic); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("topic created before storage directory was persisted: %v", err)
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(synced[len(synced)-2:], []string{root, storage}) {
		t.Fatalf("parent sync order: %v", synced)
	}
}

func TestDurableDirectoriesRetryFailedParentSync(t *testing.T) {
	root := t.TempDir()
	storage := filepath.Join(root, "storage")
	topic := filepath.Join(storage, "topic")
	failure := errors.New("parent sync failed")
	err := mkdirAllDurable(topic, func(dir string) error {
		if dir == root {
			return failure
		}

		return nil
	})
	if !errors.Is(err, failure) {
		t.Fatalf("expected sync failure, got %v", err)
	}

	if _, err := os.Stat(topic); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created child after failed parent sync: %v", err)
	}

	var retried bool
	err = mkdirAllDurable(topic, func(dir string) error {
		if dir == root {
			retried = true
		}

		return nil
	})
	if err != nil || !retried {
		t.Fatalf("retry skipped parent sync: retried=%v, err=%v", retried, err)
	}
}

func TestDurableDirectoriesRejectExistingFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}

	if err := mkdirAllDurable(file, func(string) error {
		return nil
	}); err == nil {
		t.Fatal("accepted a file as a directory")
	}
}
