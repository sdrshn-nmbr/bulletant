package client

type OperationType string

const (
	OperationPut    OperationType = "put"
	OperationDelete OperationType = "delete"
)

type TransactionOperation struct {
	Type  OperationType
	Key   []byte
	Value []byte
}

type TransactionStatus string

const (
	TransactionPending   TransactionStatus = "pending"
	TransactionCommitted TransactionStatus = "committed"
	TransactionAborted   TransactionStatus = "aborted"
)

type ScanRequest struct {
	Cursor        []byte
	Prefix        []byte
	Limit         uint32
	IncludeValues bool
	MaxValueBytes uint32
}

type ScanEntry struct {
	Key   []byte
	Value []byte
}

type ScanResult struct {
	Entries    []ScanEntry
	NextCursor []byte
}

type Vector struct {
	ID       string
	Values   []float64
	Metadata map[string]interface{}
}
