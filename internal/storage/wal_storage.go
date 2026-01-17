package storage

import (
	"errors"
	"sync"

	wal "github.com/sdrshn-nmbr/bulletant/internal/log"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type WALStorage struct {
	storage Storage
	wal     *wal.WAL
	mu      sync.Mutex
}

func OpenWALStorage(storage Storage, walPath string) (*WALStorage, error) {
	log, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	if err := log.Recover(storage); err != nil {
		_ = log.Close()
		return nil, err
	}

	return &WALStorage{
		storage: storage,
		wal:     log,
	}, nil
}

func NewWALStorage(storage Storage, wal *wal.WAL) *WALStorage {
	return &WALStorage{
		storage: storage,
		wal:     wal,
	}
}

func (w *WALStorage) Get(key types.Key) (types.Value, error) {
	return w.storage.Get(key)
}

func (w *WALStorage) Put(key types.Key, value types.Value) error {
	txn := transaction.NewTransaction()
	txn.Put(key, value)
	return w.executeLogged(txn)
}

func (w *WALStorage) Delete(key types.Key) error {
	txn := transaction.NewTransaction()
	txn.Delete(key)
	return w.executeLogged(txn)
}

func (w *WALStorage) ExecuteTransaction(t *transaction.Transaction) error {
	return w.executeLogged(t)
}

func (w *WALStorage) AddVector(values []float64, metadata map[string]interface{}) (string, error) {
	return w.storage.AddVector(values, metadata)
}

func (w *WALStorage) GetVector(id string) (*Vector, error) {
	return w.storage.GetVector(id)
}

func (w *WALStorage) DeleteVector(id string) error {
	return w.storage.DeleteVector(id)
}

func (w *WALStorage) Compact() error {
	compacter, ok := w.storage.(interface{ Compact() error })
	if !ok {
		return ErrUnsupportedOperation
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	return compacter.Compact()
}

func (w *WALStorage) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var baseErr error
	if closer, ok := w.storage.(interface{ Close() error }); ok {
		baseErr = closer.Close()
	}

	walErr := w.wal.Close()
	if baseErr != nil || walErr != nil {
		return errors.Join(baseErr, walErr)
	}
	return nil
}

func (w *WALStorage) executeLogged(t *transaction.Transaction) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.wal.LogTransaction(t); err != nil {
		return err
	}
	return w.storage.ExecuteTransaction(t)
}
