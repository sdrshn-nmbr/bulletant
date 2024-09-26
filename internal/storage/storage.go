package storage

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

// Vector represents a vector in the database
type Vector struct {
	ID       string                 `json:"id"`
	Values   []float64              `json:"values"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// VectorStore represents the vector storage
type VectorStore struct {
	vectors map[string]*Vector
	mu      sync.RWMutex
}

// NewVectorStore creates a new VectorStore
func NewVectorStore() *VectorStore {
	return &VectorStore{
		vectors: make(map[string]*Vector),
	}
}

// AddVector adds a new vector to the store
func (vs *VectorStore) AddVector(values []float64, metadata map[string]interface{}) (string, error) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	id := uuid.New().String()
	vector := &Vector{
		ID:       id,
		Values:   values,
		Metadata: metadata,
	}

	vs.vectors[id] = vector
	return id, nil
}

// GetVector retrieves a vector by its ID
func (vs *VectorStore) GetVector(id string) (*Vector, error) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	vector, ok := vs.vectors[id]
	if !ok {
		return nil, errors.New("vector not found")
	}
	return vector, nil
}

// DeleteVector removes a vector from the store
func (vs *VectorStore) DeleteVector(id string) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if _, ok := vs.vectors[id]; !ok {
		return errors.New("vector not found")
	}
	delete(vs.vectors, id)
	return nil
}

// StorageImpl represents the combined key-value and vector storage
type StorageImpl struct {
	VectorStore *VectorStore
}

// NewStorage function to initialize StorageImpl
func NewStorage() *StorageImpl {
	return &StorageImpl{
		VectorStore: NewVectorStore(),
	}
}

// Storage interface defines the methods for both key-value and vector operations
type Storage interface {
	Get(key types.Key) (types.Value, error)
	Put(key types.Key, value types.Value) error
	Delete(key types.Key) error
	ExecuteTransaction(t *transaction.Transaction) error
	AddVector(values []float64, metadata map[string]interface{}) (string, error)
	GetVector(id string) (*Vector, error)
	DeleteVector(id string) error
}

// Ensure StorageImpl implements Storage interface
var _ Storage = (*StorageImpl)(nil)

// Implement the new vector methods for StorageImpl
func (s *StorageImpl) AddVector(values []float64, metadata map[string]interface{}) (string, error) {
	return s.VectorStore.AddVector(values, metadata)
}

func (s *StorageImpl) GetVector(id string) (*Vector, error) {
	return s.VectorStore.GetVector(id)
}

func (s *StorageImpl) DeleteVector(id string) error {
	return s.VectorStore.DeleteVector(id)
}
