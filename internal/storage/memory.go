package storage

import (
	"errors"
	"hash/fnv"
	"sync"
	"time"

	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type MemoryStorage struct {
	data  map[string]types.Entry
	locks map[string]struct{}
	mu    sync.RWMutex
}

type PartitionedStorage struct {
	partitions    []*MemoryStorage
	numPartitions int
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data:  make(map[string]types.Entry),
		locks: make(map[string]struct{}),
	}
}

// ! The partitioned approach reduces contention by spreading the locks across multiple partitions
func NewPartitionedStorage(numPartitions int) *PartitionedStorage {
	ps := &PartitionedStorage{
		partitions:    make([]*MemoryStorage, numPartitions),
		numPartitions: numPartitions,
	}

	for i := 0; i < numPartitions; i++ {
		ps.partitions[i] = NewMemoryStorage()
	}

	return ps
}

func (ps *PartitionedStorage) getPartition(key types.Key) *MemoryStorage {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	partitionIdx := hash.Sum32() % uint32(ps.numPartitions)

	return ps.partitions[partitionIdx]
}

func (ps *PartitionedStorage) Get(key types.Key) (types.Value, error) {
	return ps.getPartition(key).Get(key)
}

func (ps *PartitionedStorage) Put(key types.Key, value types.Value) error {
	return ps.getPartition(key).Put(key, value)
}

func (ps *PartitionedStorage) Delte(key types.Key) error {
	return ps.getPartition(key).Delete(key)
}

func (m *MemoryStorage) Get(key types.Key) (types.Value, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.data[string(key)]
	if !ok {
		return nil, errors.New("key not found")
	}

	return entry.Value, nil
}

func (m *MemoryStorage) Put(key types.Key, value types.Value) error {
	m.mu.RLock()
	defer m.mu.Unlock()

	m.data[string(key)] = types.Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
	}

	return nil
}

func (m *MemoryStorage) Delete(key types.Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.data[string(key)]; !ok {
		return errors.New("key not found")
	}

	delete(m.data, string(key))

	return nil
}

func (m *MemoryStorage) ExecuteTransaction(txn *transaction.Transaction) error {
	// Phase 1: Prepare
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, op := range txn.Operations {
		if op.Type == types.Put {
			// Check if the key is locked by another transaction
			if _, ok := m.locks[string(op.Key)]; ok {
				txn.Status = transaction.Aborted
				return errors.New("key is locked")
			}
			m.locks[string(op.Key)] = struct{}{}
		}
	}

	// Phase 2: Commit
	for _, op := range txn.Operations {
		switch op.Type {
		case types.Put:
			m.data[string(op.Key)] = types.Entry{
				Key:       op.Key,
				Value:     op.Value,
				Timestamp: time.Now(),
			}
		case types.Delete:
			delete(m.data, string(op.Key))
		}
		delete(m.locks, string(op.Key))
	}
	txn.Status = transaction.Committed
	return nil
}
