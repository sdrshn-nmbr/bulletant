package log

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

func TestWALRecover(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer w.Close()

	txn := transaction.NewTransaction()
	txn.Put(types.Key("alpha"), []byte("one"))

	if err := w.LogBegin(txn); err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}
	if err := w.LogCommit(txn.ID); err != nil {
		t.Fatalf("LogCommit failed: %v", err)
	}

	aborted := transaction.NewTransaction()
	aborted.Put(types.Key("beta"), []byte("two"))
	if err := w.LogBegin(aborted); err != nil {
		t.Fatalf("LogBegin failed: %v", err)
	}
	if err := w.LogAbort(aborted.ID); err != nil {
		t.Fatalf("LogAbort failed: %v", err)
	}

	recovered := storage.NewMemoryStorage()
	if err := w.Recover(recovered); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	value, err := recovered.Get(types.Key("alpha"))
	if err != nil {
		t.Fatalf("Expected alpha after recovery, got %v", err)
	}
	if string(value) != "one" {
		t.Fatalf("Expected one, got %s", string(value))
	}

	_, err = recovered.Get(types.Key("beta"))
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Expected ErrKeyNotFound for beta, got %v", err)
	}
}
