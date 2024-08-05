package transaction

import (
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

type OperationType int

const (
	Get    OperationType = iota // 0
	Put                         // 1
	Delete                      // 2
)

type Operation struct {
	Type  OperationType
	Key   storage.Key
	Value storage.Value
}

type TransactionStatus int

const (
	Pending   TransactionStatus = iota // 0
	Committed                          // 1
	Aborted                            // 2
)

type Transaction struct {
	Operations []Operation
	Status     TransactionStatus
}

func NewTransaction() *Transaction {
	return &Transaction{
		Operations: make([]Operation, 0),
		Status:     Pending,
	}
}

func (t *Transaction) Put(key storage.Key, val storage.Value) {
	t.Operations = append(t.Operations, Operation{
		Type:  Put,
		Key:   key,
		Value: val,
	})
}
func (t *Transaction) Delete(key storage.Key, val storage.Value) {
	t.Operations = append(t.Operations, Operation{
		Type:  Delete,
		Key:   key,
	})
}
