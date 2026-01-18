package lsm

import (
	"path/filepath"
	"testing"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

func TestLSMPutGetDelete(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultLSMOptions(dir)
	opts.MemtableMaxEntries = 1
	opts.MemtableMaxBytes = 64

	store, err := NewLSMStorage(opts)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()

	if err := store.Put(types.Key("alpha"), types.Value("one")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	value, err := store.Get(types.Key("alpha"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(value) != "one" {
		t.Fatalf("expected one, got %s", string(value))
	}

	if err := store.Delete(types.Key("alpha")); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, err := store.Get(types.Key("alpha")); err == nil {
		t.Fatalf("expected key not found after delete")
	}
}

func TestLSMScanPrefix(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultLSMOptions(dir)
	opts.MemtableMaxEntries = 2

	store, err := NewLSMStorage(opts)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()

	_ = store.Put(types.Key("alpha"), types.Value("one"))
	_ = store.Put(types.Key("beta"), types.Value("two"))
	_ = store.Put(types.Key("gamma"), types.Value("three"))

	result, err := store.Scan(storage.ScanRequest{
		Prefix:        types.Key("b"),
		Limit:         10,
		IncludeValues: true,
		MaxValueBytes: 64,
	})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result.Entries))
	}
	if string(result.Entries[0].Key) != "beta" {
		t.Fatalf("expected beta, got %s", string(result.Entries[0].Key))
	}
}

func TestBloomFilterNoFalseNegatives(t *testing.T) {
	filter, err := newBloomFilter(10, 10)
	if err != nil {
		t.Fatalf("bloom init failed: %v", err)
	}
	filter.add([]byte("alpha"))
	if !filter.mayContain([]byte("alpha")) {
		t.Fatalf("expected bloom to contain key")
	}
}

func TestLSMCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultLSMOptions(dir)
	opts.MemtableMaxEntries = 1
	opts.MemtableMaxBytes = 64
	opts.CompactionMaxSegments = 2

	store, err := NewLSMStorage(opts)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()

	_ = store.Put(types.Key("alpha"), types.Value("one"))
	_ = store.Put(types.Key("beta"), types.Value("two"))
	_ = store.Put(types.Key("gamma"), types.Value("three"))

	before := len(store.segments)
	if before < 2 {
		t.Fatalf("expected multiple segments, got %d", before)
	}

	_, err = store.Compact(storage.CompactOptions{
		MaxEntries: 100,
		MaxBytes:   1 << 20,
	})
	if err != nil {
		t.Fatalf("compact failed: %v", err)
	}
	after := len(store.segments)
	if after >= before {
		t.Fatalf("expected fewer segments after compaction")
	}

	value, err := store.Get(types.Key("beta"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(value) != "two" {
		t.Fatalf("expected two, got %s", string(value))
	}
}

func TestLSMSnapshotRestore(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultLSMOptions(dir)
	opts.MemtableMaxEntries = 1
	store, err := NewLSMStorage(opts)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer store.Close()

	if err := store.Put(types.Key("alpha"), types.Value("one")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	snapshotDir := filepath.Join(t.TempDir(), "snapshot")
	if _, err := store.Snapshot(storage.SnapshotOptions{Path: snapshotDir}); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	restore, err := NewLSMStorage(DefaultLSMOptions(snapshotDir))
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
