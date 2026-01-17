package storage

import (
	"errors"
	"testing"

	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

func TestMemoryStorage(t *testing.T) {
	store := NewMemoryStorage()

	// Test Put and Get
	err := store.Put(types.Key("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	value, err := store.Get(types.Key("key1"))
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if string(value) != "value1" {
		t.Fatalf("Expected 'value1', got '%s'", string(value))
	}

	// Test Delete
	err = store.Delete(types.Key("key1"))
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	_, err = store.Get(types.Key("key1"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Expected ErrKeyNotFound after delete, got %v", err)
	}
}
