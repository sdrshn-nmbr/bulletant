package client

import (
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
