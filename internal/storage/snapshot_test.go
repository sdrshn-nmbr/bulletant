package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

func TestMemorySnapshotFile(t *testing.T) {
	store := NewMemoryStorage()
	if err := store.Put(types.Key("alpha"), []byte("one")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := store.Put(types.Key("beta"), []byte("two")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	path := filepath.Join(t.TempDir(), "snapshot.bin")
	stats, err := store.Snapshot(SnapshotOptions{Path: path})
	if err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}
	if stats.Entries != 2 {
		t.Fatalf("expected 2 entries, got %d", stats.Entries)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot failed: %v", err)
	}
	if len(data) < 9 {
		t.Fatalf("snapshot too small")
	}
	if string(data[:4]) != memorySnapshotMagic {
		t.Fatalf("unexpected magic")
	}
	count := binary.LittleEndian.Uint32(data[5:9])
	if count != 2 {
		t.Fatalf("expected count 2, got %d", count)
	}
}

func TestDiskSnapshotRestore(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data.db")
	store, err := NewDiskStorage(dataPath)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()

	if err := store.Put(types.Key("alpha"), []byte("one")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	snapshotPath := filepath.Join(dir, "snapshot.db")
	if _, err := store.Snapshot(SnapshotOptions{Path: snapshotPath}); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	restore, err := NewDiskStorage(snapshotPath)
	if err != nil {
		t.Fatalf("restore open failed: %v", err)
	}
	defer restore.Close()

	value, err := restore.Get(types.Key("alpha"))
	if err != nil {
		t.Fatalf("restore get failed: %v", err)
	}
	if string(value) != "one" {
		t.Fatalf("expected one, got %s", string(value))
	}
}
