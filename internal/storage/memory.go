package storage

import (
	"hash/fnv"
	"sync"
	"time"

	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type MemoryStorage struct {
	data        map[string]types.Entry
	locks       map[string]struct{}
	mu          sync.RWMutex
	vectorStore *VectorStore
}

type PartitionedStorage struct {
	partitions    []*MemoryStorage
	numPartitions int
	vectorStore   *VectorStore
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data:        make(map[string]types.Entry),
		locks:       make(map[string]struct{}),
		vectorStore: NewVectorStore(),
	}
}

// Vector operations
func (m *MemoryStorage) AddVector(values []float64, metadata map[string]interface{}) (string, error) {
	return m.vectorStore.AddVector(values, metadata)
}

func (m *MemoryStorage) GetVector(id string) (*Vector, error) {
	return m.vectorStore.GetVector(id)
}

func (m *MemoryStorage) DeleteVector(id string) error {
	return m.vectorStore.DeleteVector(id)
}

// ! The partitioned approach reduces contention by spreading the locks across multiple partitions
func NewPartitionedStorage(numPartitions int) *PartitionedStorage {
	if numPartitions <= 0 {
		numPartitions = 1
	}
	ps := &PartitionedStorage{
		partitions:    make([]*MemoryStorage, numPartitions),
		numPartitions: numPartitions,
		vectorStore:   NewVectorStore(),
	}

	for i := 0; i < numPartitions; i++ {
		ps.partitions[i] = NewMemoryStorage()
	}

	return ps
}

func (ps *PartitionedStorage) getPartition(key types.Key) *MemoryStorage {
	return ps.partitions[ps.getPartitionIndex(key)]
}

func (ps *PartitionedStorage) getPartitionIndex(key types.Key) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	partitionIdx := hash.Sum32() % uint32(ps.numPartitions)

	return int(partitionIdx)
}

func (ps *PartitionedStorage) ExecuteTransaction(t *transaction.Transaction) error {
	// Group ops by partition
	partitionOps := make(map[int][]transaction.Operation)
	for _, op := range t.Operations {
		idx := ps.getPartitionIndex(op.Key)
		partitionOps[idx] = append(partitionOps[idx], op)
	}

	// Execute transaction on each partition
	for i, ops := range partitionOps {
		partitionTxn := transaction.NewTransaction()
		partitionTxn.Operations = ops
		err := ps.partitions[i].ExecuteTransaction(partitionTxn)
		if err != nil {
			t.Status = transaction.Aborted
			return err
		}
	}

	t.Status = transaction.Committed

	return nil

}

func (m *MemoryStorage) BatchPut(entries []types.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, entry := range entries {
		m.data[string(entry.Key)] = entry
	}

	return nil
}

func (ps *PartitionedStorage) Get(key types.Key) (types.Value, error) {
	return ps.getPartition(key).Get(key)
}

func (ps *PartitionedStorage) Put(key types.Key, value types.Value) error {
	return ps.getPartition(key).Put(key, value)
}

func (ps *PartitionedStorage) Delete(key types.Key) error {
	return ps.getPartition(key).Delete(key)
}

func (ps *PartitionedStorage) AddVector(values []float64, metadata map[string]interface{}) (string, error) {
	return ps.vectorStore.AddVector(values, metadata)
}

func (ps *PartitionedStorage) GetVector(id string) (*Vector, error) {
	return ps.vectorStore.GetVector(id)
}

func (ps *PartitionedStorage) DeleteVector(id string) error {
	return ps.vectorStore.DeleteVector(id)
}

func (m *MemoryStorage) Get(key types.Key) (types.Value, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.data[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return entry.Value, nil
}

func (m *MemoryStorage) Put(key types.Key, value types.Value) error {
	m.mu.Lock()
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
		return ErrKeyNotFound
	}

	delete(m.data, string(key))

	return nil
}

func (m *MemoryStorage) ExecuteTransaction(t *transaction.Transaction) error {
	// Phase 1: Prep
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, op := range t.Operations {
		// Check if key is locked by another op
		if _, ok := m.locks[string(op.Key)]; ok {
			t.Status = transaction.Aborted
			return ErrKeyLocked
		}
		m.locks[string(op.Key)] = struct{}{}
	}

	// Phase 2: Commit
	for _, op := range t.Operations {
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
	}
	t.Status = transaction.Committed

	// Cleanup: Release locks
	for _, op := range t.Operations {
		delete(m.locks, string(op.Key))
	}

	return nil
}

func (m *MemoryStorage) Close() error {
	return nil
}

func (ps *PartitionedStorage) Close() error {
	for _, partition := range ps.partitions {
		if err := partition.Close(); err != nil {
			return err
		}
	}
	return nil
}
