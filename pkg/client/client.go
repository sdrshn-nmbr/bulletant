package main

import (
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type Client struct {
	storage storage.Storage
}

func NewClient(storage storage.Storage) *Client {
	return &Client{storage: storage}
}

func (c *Client) Get(key []byte) ([]byte, error) {
	return c.storage.Get(types.Key(key))
}

func (c *Client) Put(key []byte, val []byte) error {
	return c.storage.Put(types.Key(key), types.Value(val))
}

func (c *Client) Delete(key []byte) error {
	return c.storage.Delete(types.Key(key))
}

func (c *Client) Transaction(fn func(*transaction.Transaction)) error {
	txn := transaction.NewTransaction()
	fn(txn)

	return c.storage.ExecuteTransaction(txn)
}
