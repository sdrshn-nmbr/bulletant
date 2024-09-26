package client

import (
	"encoding/json"
	"errors"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type Client struct {
	Storage storage.Storage
}

func NewClient(storage storage.Storage) *Client {
	return &Client{Storage: storage}
}

func (c *Client) Get(key []byte) ([]byte, error) {
	return c.Storage.Get(types.Key(key))
}

func (c *Client) Put(key []byte, val []byte) error {
	return c.Storage.Put(types.Key(key), types.Value(val))
}

func (c *Client) Delete(key []byte) error {
	return c.Storage.Delete(types.Key(key))
}

func (c *Client) Transaction(fn func(*transaction.Transaction)) (transaction.TransactionStatus, error) {
	txn := transaction.NewTransaction()
	fn(txn)

	err := c.Storage.ExecuteTransaction(txn)
	return txn.Status, err
}

// AddVector adds a new vector to the database
func (c *Client) AddVector(values []float64, metadata map[string]interface{}) (string, error) {
	return c.Storage.AddVector(values, metadata)
}

// GetVector retrieves a vector by its ID
func (c *Client) GetVector(id string) (*storage.Vector, error) {
	return c.Storage.GetVector(id)
}

// DeleteVector removes a vector from the database
func (c *Client) DeleteVector(id string) error {
	return c.Storage.DeleteVector(id)
}
