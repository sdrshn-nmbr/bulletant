package db

import (
	"errors"

	wal "github.com/sdrshn-nmbr/bulletant/internal/log"
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type Options struct {
	Storage storage.Storage
	WAL     *wal.WAL
}

type DB struct {
	storage storage.Storage
	wal     *wal.WAL
}

func Open(opts Options) (*DB, error) {
	if opts.Storage == nil {
		return nil, ErrStorageRequired
	}

	if opts.WAL != nil {
		if err := opts.WAL.Recover(opts.Storage); err != nil {
			return nil, errors.Join(ErrRecoveryFailed, err)
		}
	}

	return &DB{
		storage: opts.Storage,
		wal:     opts.WAL,
	}, nil
}

func (d *DB) Get(key []byte) ([]byte, error) {
	return d.storage.Get(types.Key(key))
}

func (d *DB) Put(key []byte, value []byte) error {
	txn := transaction.NewTransaction()
	txn.Put(types.Key(key), types.Value(value))
	return d.ExecuteTransaction(txn)
}

func (d *DB) Delete(key []byte) error {
	txn := transaction.NewTransaction()
	txn.Delete(types.Key(key))
	return d.ExecuteTransaction(txn)
}

func (d *DB) Transaction(fn func(*transaction.Transaction)) (transaction.TransactionStatus, error) {
	txn := transaction.NewTransaction()
	fn(txn)
	return txn.Status, d.ExecuteTransaction(txn)
}

func (d *DB) ExecuteTransaction(txn *transaction.Transaction) error {
	if txn == nil {
		return ErrInvalidTxn
	}

	if d.wal != nil {
		if err := d.wal.LogBegin(txn); err != nil {
			return errors.Join(ErrWALBeginFailed, err)
		}
	}

	if err := d.storage.ExecuteTransaction(txn); err != nil {
		if d.wal != nil {
			if abortErr := d.wal.LogAbort(txn.ID); abortErr != nil {
				return errors.Join(err, ErrWALAbortFailed, abortErr)
			}
		}
		return err
	}

	if d.wal != nil {
		if err := d.wal.LogCommit(txn.ID); err != nil {
			return errors.Join(ErrWALCommitFailed, err)
		}
	}

	return nil
}

func (d *DB) AddVector(values []float64, metadata map[string]interface{}) (string, error) {
	return d.storage.AddVector(values, metadata)
}

func (d *DB) GetVector(id string) (*storage.Vector, error) {
	return d.storage.GetVector(id)
}

func (d *DB) DeleteVector(id string) error {
	return d.storage.DeleteVector(id)
}

func (d *DB) Close() error {
	var errs []error
	if d.wal != nil {
		if err := d.wal.Close(); err != nil {
			errs = append(errs, errors.Join(ErrWALCloseFail, err))
		}
	}
	if err := d.storage.Close(); err != nil {
		errs = append(errs, errors.Join(ErrStorageCloseFail, err))
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}
