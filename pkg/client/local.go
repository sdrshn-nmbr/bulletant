package client

import (
	"context"
	"errors"

	"github.com/sdrshn-nmbr/bulletant/internal/db"
	wal "github.com/sdrshn-nmbr/bulletant/internal/log"
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type StorageType string

const (
	StorageMemory      StorageType = "memory"
	StorageDisk        StorageType = "disk"
	StoragePartitioned StorageType = "partitioned"
)

type LocalOptions struct {
	StorageType StorageType
	DataPath    string
	WALPath     string
	Partitions  uint32
}

type LocalClient struct {
	db *db.DB
}

func OpenLocal(opts LocalOptions) (*LocalClient, error) {
	store, err := buildStorage(opts)
	if err != nil {
		return nil, err
	}

	var walLog *wal.WAL
	if opts.WALPath != "" {
		walLog, err = wal.NewWAL(opts.WALPath)
		if err != nil {
			return nil, err
		}
	}

	database, err := db.Open(db.Options{
		Storage: store,
		WAL:     walLog,
	})
	if err != nil {
		return nil, err
	}

	return &LocalClient{db: database}, nil
}

func (c *LocalClient) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	value, err := c.db.Get(key)
	if err != nil {
		return nil, mapStorageError(err)
	}
	return value, nil
}

func (c *LocalClient) Put(ctx context.Context, key []byte, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := c.db.Put(key, value); err != nil {
		return mapStorageError(err)
	}
	return nil
}

func (c *LocalClient) Delete(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := c.db.Delete(key); err != nil {
		return mapStorageError(err)
	}
	return nil
}

func (c *LocalClient) ExecuteTransaction(
	ctx context.Context,
	ops []TransactionOperation,
) (TransactionStatus, error) {
	if err := ctx.Err(); err != nil {
		return TransactionAborted, err
	}
	if len(ops) == 0 {
		return TransactionAborted, ErrInvalidArgument
	}

	txn := transaction.NewTransaction()
	for _, op := range ops {
		if err := applyOperation(txn, op); err != nil {
			return TransactionAborted, err
		}
	}

	if err := c.db.ExecuteTransaction(txn); err != nil {
		return TransactionAborted, mapStorageError(err)
	}

	return TransactionStatus(txn.Status.String()), nil
}

func (c *LocalClient) Scan(
	ctx context.Context,
	req ScanRequest,
) (ScanResult, error) {
	if err := ctx.Err(); err != nil {
		return ScanResult{}, err
	}

	storageReq, err := toStorageScanRequest(req)
	if err != nil {
		return ScanResult{}, err
	}

	result, err := c.db.Scan(storageReq)
	if err != nil {
		return ScanResult{}, mapStorageError(err)
	}

	return fromStorageScanResult(result), nil
}

func (c *LocalClient) AddVector(
	ctx context.Context,
	values []float64,
	metadata map[string]interface{},
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	id, err := c.db.AddVector(values, metadata)
	if err != nil {
		return "", mapStorageError(err)
	}
	return id, nil
}

func (c *LocalClient) GetVector(ctx context.Context, id string) (*Vector, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	vector, err := c.db.GetVector(id)
	if err != nil {
		return nil, mapStorageError(err)
	}

	return &Vector{
		ID:       vector.ID,
		Values:   vector.Values,
		Metadata: vector.Metadata,
	}, nil
}

func (c *LocalClient) DeleteVector(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := c.db.DeleteVector(id); err != nil {
		return mapStorageError(err)
	}
	return nil
}

func (c *LocalClient) Compact(
	ctx context.Context,
	opts CompactOptions,
) (CompactStats, error) {
	if err := ctx.Err(); err != nil {
		return CompactStats{}, err
	}

	stats, err := c.db.Compact(storage.CompactOptions{
		MaxEntries: opts.MaxEntries,
		MaxBytes:   opts.MaxBytes,
		TempPath:   opts.TempPath,
	})
	if err != nil {
		return CompactStats{}, mapStorageError(err)
	}

	return CompactStats{
		EntriesTotal:   stats.EntriesTotal,
		EntriesWritten: stats.EntriesWritten,
		BytesBefore:    stats.BytesBefore,
		BytesAfter:     stats.BytesAfter,
	}, nil
}

func (c *LocalClient) Close() error {
	return c.db.Close()
}

func buildStorage(opts LocalOptions) (storage.Storage, error) {
	switch opts.StorageType {
	case StorageMemory:
		return storage.NewMemoryStorage(), nil
	case StoragePartitioned:
		if opts.Partitions == 0 {
			return nil, ErrInvalidArgument
		}
		maxInt := int(^uint(0) >> 1)
		if opts.Partitions > uint32(maxInt) {
			return nil, ErrInvalidArgument
		}
		return storage.NewPartitionedStorage(int(opts.Partitions)), nil
	case StorageDisk:
		if opts.DataPath == "" {
			return nil, ErrInvalidArgument
		}
		return storage.NewDiskStorage(opts.DataPath)
	default:
		return nil, ErrInvalidArgument
	}
}

func applyOperation(txn *transaction.Transaction, op TransactionOperation) error {
	switch op.Type {
	case OperationPut:
		txn.Put(types.Key(op.Key), types.Value(op.Value))
		return nil
	case OperationDelete:
		txn.Delete(types.Key(op.Key))
		return nil
	default:
		return ErrInvalidArgument
	}
}

func toStorageScanRequest(req ScanRequest) (storage.ScanRequest, error) {
	if req.Limit == 0 {
		return storage.ScanRequest{}, ErrInvalidArgument
	}
	if req.IncludeValues && req.MaxValueBytes == 0 {
		return storage.ScanRequest{}, ErrInvalidArgument
	}

	storageReq := storage.ScanRequest{
		Cursor:        types.Key(req.Cursor),
		Prefix:        types.Key(req.Prefix),
		Limit:         req.Limit,
		IncludeValues: req.IncludeValues,
		MaxValueBytes: req.MaxValueBytes,
	}
	if err := storageReq.Validate(); err != nil {
		return storage.ScanRequest{}, ErrInvalidArgument
	}
	return storageReq, nil
}

func fromStorageScanResult(result storage.ScanResult) ScanResult {
	entries := make([]ScanEntry, 0, len(result.Entries))
	for _, entry := range result.Entries {
		entries = append(entries, ScanEntry{
			Key:   []byte(entry.Key),
			Value: entry.Value,
		})
	}

	return ScanResult{
		Entries:    entries,
		NextCursor: []byte(result.NextCursor),
	}
}

func mapStorageError(err error) error {
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		return ErrKeyNotFound
	case errors.Is(err, storage.ErrVectorNotFound):
		return ErrVectorNotFound
	case errors.Is(err, storage.ErrKeyLocked):
		return ErrKeyLocked
	case errors.Is(err, storage.ErrInvalidScanLimit),
		errors.Is(err, storage.ErrInvalidValueLimit),
		errors.Is(err, storage.ErrInvalidCompactLimit):
		return ErrInvalidArgument
	case errors.Is(err, storage.ErrValueTooLarge):
		return ErrInvalidArgument
	case errors.Is(err, storage.ErrCompactLimitExceeded):
		return ErrConflict
	case errors.Is(err, storage.ErrUnsupported):
		return ErrUnsupported
	default:
		return err
	}
}
