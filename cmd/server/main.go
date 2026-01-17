package main

import (
	"errors"
	"flag"
	"fmt"
	"log"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

func main() {
	backend := flag.String("backend", "memory", "storage backend: memory|disk|partitioned")
	dataPath := flag.String("data", "bulletant.data", "disk storage path")
	walPath := flag.String("wal", "", "WAL path (enables durability)")
	partitions := flag.Int("partitions", 4, "partitions for partitioned backend")
	compact := flag.Bool("compact", false, "compact disk storage before exit")
	flag.Parse()

	store, err := buildStorage(*backend, *dataPath, *walPath, *partitions)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	if closer, ok := store.(interface{ Close() error }); ok {
		defer func() {
			if err := closer.Close(); err != nil {
				log.Printf("Storage close error: %v", err)
			}
		}()
	}

	c := client.NewClient(store)

	// Put a key-value pair
	if err := c.Put([]byte("name"), []byte("John Doe")); err != nil {
		log.Fatalf("Failed to put: %v", err)
	}

	// Get the value for a key
	value, err := c.Get([]byte("name"))
	if err != nil {
		log.Fatalf("Failed to get: %v", err)
	}
	fmt.Printf("Name: %s\n", string(value))

	// Update the value using a transaction
	status, err := c.Transaction(func(txn *transaction.Transaction) {
		txn.Put(types.Key("name"), []byte("Jane Doe"))
		txn.Put(types.Key("age"), []byte("30"))
	})
	if err != nil {
		log.Fatalf("Transaction failed: %v", err)
	}
	fmt.Printf("Transaction status: %v\n", status)

	// Get the updated values
	name, _ := c.Get([]byte("name"))
	age, _ := c.Get([]byte("age"))
	fmt.Printf("Updated name: %s\n", string(name))
	fmt.Printf("Age: %s\n", string(age))

	// Delete a key
	if err := c.Delete([]byte("age")); err != nil {
		log.Fatalf("Failed to delete: %v", err)
	}

	// Try to get the deleted key
	_, err = c.Get([]byte("age"))
	if errors.Is(err, storage.ErrKeyNotFound) {
		fmt.Println("Age was successfully deleted")
	}

	// Vector operations (supported in memory + partitioned backends)
	vecID, err := c.AddVector([]float64{0.1, 0.2, 0.3}, map[string]interface{}{"source": "demo"})
	if err != nil {
		if errors.Is(err, storage.ErrVectorUnsupported) {
			fmt.Println("Vector operations are not supported by this backend")
		} else {
			log.Fatalf("Failed to add vector: %v", err)
		}
	} else {
		vec, err := c.GetVector(vecID)
		if err != nil {
			log.Fatalf("Failed to get vector: %v", err)
		}
		fmt.Printf("Vector %s: values=%v metadata=%v\n", vec.ID, vec.Values, vec.Metadata)
	}

	if *compact {
		if compacter, ok := store.(interface{ Compact() error }); ok {
			if err := compacter.Compact(); err != nil && !errors.Is(err, storage.ErrUnsupportedOperation) {
				log.Printf("Compaction failed: %v", err)
			}
		} else {
			fmt.Println("Compaction not supported by this backend")
		}
	}
}

func buildStorage(backend, dataPath, walPath string, partitions int) (storage.Storage, error) {
	var store storage.Storage

	switch backend {
	case "memory":
		store = storage.NewMemoryStorage()
	case "partitioned":
		store = storage.NewPartitionedStorage(partitions)
	case "disk":
		disk, err := storage.NewDiskStorage(dataPath)
		if err != nil {
			return nil, err
		}
		store = disk
	default:
		return nil, fmt.Errorf("unknown backend %q", backend)
	}

	if walPath != "" {
		return storage.OpenWALStorage(store, walPath)
	}

	return store, nil
}
