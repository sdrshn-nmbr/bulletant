package transaction

import (
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type Operation struct {
	Type  types.OperationType
	Key   types.Key
	Value types.Value
}

type TransactionStatus int

const (
	Pending TransactionStatus = iota
	Committed
	Aborted
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

func (t *Transaction) Put(key types.Key, val types.Value) {
	t.Operations = append(t.Operations, Operation{
		Type:  types.Put,
		Key:   key,
		Value: val,
	})
}

func (t *Transaction) Delete(key types.Key) {
	t.Operations = append(t.Operations, Operation{
		Type: types.Delete,
		Key:  key,
	})
}
