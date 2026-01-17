package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

func TestDiskStoragePutGetDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.db")

	store, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("failed to create disk storage: %v", err)
	}
	defer store.Close()

	if err := store.Put(types.Key("k1"), []byte("v1")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	value, err := store.Get(types.Key("k1"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(value) != "v1" {
		t.Fatalf("expected v1, got %s", string(value))
	}

	if err := store.Put(types.Key("k1"), []byte("v2")); err != nil {
		t.Fatalf("put overwrite failed: %v", err)
	}

	value, err = store.Get(types.Key("k1"))
	if err != nil {
		t.Fatalf("get after overwrite failed: %v", err)
	}
	if string(value) != "v2" {
		t.Fatalf("expected v2, got %s", string(value))
	}

	if err := store.Delete(types.Key("k1")); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	_, err = store.Get(types.Key("k1"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDiskStorageTransactionAtomicity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.db")

	store, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("failed to create disk storage: %v", err)
	}
	defer store.Close()

	if err := store.Put(types.Key("keep"), []byte("stable")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	txn := transaction.NewTransaction()
	txn.Delete(types.Key("missing"))
	txn.Put(types.Key("new"), []byte("value"))

	if err := store.ExecuteTransaction(txn); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	if _, err := store.Get(types.Key("new")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected new key to be absent, got %v", err)
	}

	txn2 := transaction.NewTransaction()
	txn2.Put(types.Key("temp"), []byte("1"))
	txn2.Delete(types.Key("temp"))

	if err := store.ExecuteTransaction(txn2); err != nil {
		t.Fatalf("transaction failed: %v", err)
	}

	if _, err := store.Get(types.Key("temp")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected temp to be deleted, got %v", err)
	}
}

func TestDiskStorageCompact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.db")

	store, err := NewDiskStorage(path)
	if err != nil {
		t.Fatalf("failed to create disk storage: %v", err)
	}
	defer store.Close()

	if err := store.Put(types.Key("k1"), []byte("v1")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := store.Put(types.Key("k2"), []byte("v2")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := store.Delete(types.Key("k1")); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat before compact failed: %v", err)
	}

	if err := store.Compact(); err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after compact failed: %v", err)
	}

	if after.Size() >= before.Size() {
		t.Fatalf("expected compaction to reduce size, before=%d after=%d", before.Size(), after.Size())
	}

	value, err := store.Get(types.Key("k2"))
	if err != nil {
		t.Fatalf("get after compact failed: %v", err)
	}
	if string(value) != "v2" {
		t.Fatalf("expected v2, got %s", string(value))
	}

	if _, err := store.Get(types.Key("k1")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected k1 to be deleted, got %v", err)
	}
}
