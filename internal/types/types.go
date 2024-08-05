package types

import "time"

type Key string
type Value []byte

type Entry struct {
	Key       Key
	Value     Value
	Timestamp time.Time
}

type OperationType int

const (
	Get OperationType = iota
	Put
	Delete
)
