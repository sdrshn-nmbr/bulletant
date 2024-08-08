package main

import (
	"fmt"
	"log"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

func main() {
	// Create a new memory storage
	memStorage := storage.NewMemoryStorage()

	// Create a new client
	c := client.NewClient(memStorage)

	// Put a key-value pair
	err := c.Put([]byte("name"), []byte("John Doe"))
	if err != nil {
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
	err = c.Delete([]byte("age"))
	if err != nil {
		log.Fatalf("Failed to delete: %v", err)
	}

	// Try to get the deleted key
	_, err = c.Get([]byte("age"))
	if err != nil {
		fmt.Println("Age was successfully deleted")
	}
}
