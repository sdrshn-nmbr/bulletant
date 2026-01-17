package storage

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

func TestDiskStoragePutGetDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bulletant.db")

	store, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("NewDiskStorage failed: %v", err)
	}
	defer store.Close()

	if err := store.Put(types.Key("user"), []byte("alice")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	value, err := store.Get(types.Key("user"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "alice" {
		t.Fatalf("Expected alice, got %s", string(value))
	}

	if err := store.Put(types.Key("user"), []byte("bob")); err != nil {
		t.Fatalf("Put update failed: %v", err)
	}

	value, err = store.Get(types.Key("user"))
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	if string(value) != "bob" {
		t.Fatalf("Expected bob, got %s", string(value))
	}

	if err := store.Delete(types.Key("user")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = store.Get(types.Key("user"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestDiskStorageReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bulletant.db")

	store, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("NewDiskStorage failed: %v", err)
	}

	if err := store.Put(types.Key("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer reopened.Close()

	value, err := reopened.Get(types.Key("alpha"))
	if err != nil {
		t.Fatalf("Get after reopen failed: %v", err)
	}
	if string(value) != "one" {
		t.Fatalf("Expected one, got %s", string(value))
	}
}

func TestDiskStorageCompact(t *testing.T) {
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
	if err := store.Put(types.Key("alpha"), []byte("two")); err != nil {
		t.Fatalf("Put update failed: %v", err)
	}
	if err := store.Put(types.Key("beta"), []byte("three")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := store.Delete(types.Key("beta")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	bytesBefore := uint64(store.size)
	stats, err := store.Compact(CompactOptions{
		MaxEntries: 8,
		MaxBytes:   bytesBefore,
	})
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	if stats.BytesAfter > stats.BytesBefore {
		t.Fatalf("Expected compaction to shrink or keep size")
	}

	value, err := store.Get(types.Key("alpha"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "two" {
		t.Fatalf("Expected two, got %s", string(value))
	}

	_, err = store.Get(types.Key("beta"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Expected ErrKeyNotFound, got %v", err)
	}
}
