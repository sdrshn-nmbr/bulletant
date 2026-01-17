package transaction

import (
	"github.com/google/uuid"
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

func (s TransactionStatus) String() string {
	switch s {
	case Pending:
		return "pending"
	case Committed:
		return "committed"
	case Aborted:
		return "aborted"
	default:
		return "unknown"
	}
}

type Transaction struct {
	ID         uuid.UUID
	Operations []Operation
	Status     TransactionStatus
}

func NewTransaction() *Transaction {
	return &Transaction{
		ID:         uuid.New(),
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

// No Get method as when transacting, we only consider methods that change the data itself -> Get does not change the data
