package storage

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

func TestMemoryStorageScan(t *testing.T) {
	store := NewMemoryStorage()

	if err := store.Put(types.Key("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := store.Put(types.Key("beta"), []byte("two")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := store.Put(types.Key("alphabet"), []byte("three")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	result, err := store.Scan(ScanRequest{
		Prefix:        types.Key("alp"),
		Limit:         1,
		IncludeValues: true,
		MaxValueBytes: 8,
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(result.Entries))
	}
	if result.Entries[0].Key != types.Key("alpha") {
		t.Fatalf("Expected alpha, got %s", result.Entries[0].Key)
	}
	if result.NextCursor == "" {
		t.Fatalf("Expected next cursor")
	}

	next, err := store.Scan(ScanRequest{
		Cursor:        result.NextCursor,
		Prefix:        types.Key("alp"),
		Limit:         1,
		IncludeValues: false,
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(next.Entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(next.Entries))
	}
	if next.Entries[0].Key != types.Key("alphabet") {
		t.Fatalf("Expected alphabet, got %s", next.Entries[0].Key)
	}
}

func TestDiskStorageScan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bulletant.db")

	store, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("NewDiskStorage failed: %v", err)
	}
	defer store.Close()

	if err := store.Put(types.Key("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	_, err = store.Scan(ScanRequest{
		Limit:         1,
		IncludeValues: true,
		MaxValueBytes: 1,
	})
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("Expected ErrValueTooLarge, got %v", err)
	}
}
