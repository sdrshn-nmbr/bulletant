package storage

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

func TestWALStorageRecovery(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "store.wal")

	base := NewMemoryStorage()
	wrapped, err := OpenWALStorage(base, walPath)
	if err != nil {
		t.Fatalf("failed to open WAL storage: %v", err)
	}

	if err := wrapped.Put(types.Key("name"), []byte("Ada")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	txn := transaction.NewTransaction()
	txn.Put(types.Key("role"), []byte("engineer"))
	txn.Delete(types.Key("name"))
	if err := wrapped.ExecuteTransaction(txn); err != nil {
		t.Fatalf("transaction failed: %v", err)
	}

	if err := wrapped.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	recoveredBase := NewMemoryStorage()
	recovered, err := OpenWALStorage(recoveredBase, walPath)
	if err != nil {
		t.Fatalf("failed to reopen WAL storage: %v", err)
	}
	defer recovered.Close()

	if _, err := recovered.Get(types.Key("name")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected name to be deleted after recovery, got %v", err)
	}

	role, err := recovered.Get(types.Key("role"))
	if err != nil {
		t.Fatalf("failed to get role after recovery: %v", err)
	}
	if string(role) != "engineer" {
		t.Fatalf("expected engineer, got %s", string(role))
	}
}
