package storage

import (
	"errors"
	"testing"

	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

func TestPartitionedStorageBasic(t *testing.T) {
	store := NewPartitionedStorage(4)
	defer store.Close()

	if err := store.Put(types.Key("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	value, err := store.Get(types.Key("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "value1" {
		t.Fatalf("Expected value1, got %s", string(value))
	}

	if err := store.Delete(types.Key("key1")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = store.Get(types.Key("key1"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestPartitionedStorageVectors(t *testing.T) {
	store := NewPartitionedStorage(2)
	defer store.Close()

	id, err := store.AddVector([]float64{0.1, 0.2}, map[string]interface{}{"label": "test"})
	if err != nil {
		t.Fatalf("AddVector failed: %v", err)
	}

	vector, err := store.GetVector(id)
	if err != nil {
		t.Fatalf("GetVector failed: %v", err)
	}
	if len(vector.Values) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(vector.Values))
	}
}
