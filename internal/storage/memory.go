package storage

import (
	"errors"
	"sync"
	"time"
)

type MemoryStorage struct {
	data map[string]Entry
	mu   sync.RWMutex
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string]Entry),
	}
}

func (m *MemoryStorage) Get(key Key) (Value, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.data[string(key)]
	if !ok {
		return nil, errors.New("Key not found")
	}

	return entry.Value, nil
}

func (m *MemoryStorage) Put(key Key, value Value) error {
	m.mu.RLock()
	defer m.mu.Unlock()

	m.data[string(key)] = Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
	}

	return nil
}

func (m *MemoryStorage) Delete(key Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.data[string(key)]; !ok {
		return errors.New("Key not found")
	}

	delete(m.data, string(key))

	return nil
}
