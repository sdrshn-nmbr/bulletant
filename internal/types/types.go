package types

import (
	"fmt"
	"strings"
	"time"
)

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

func (o OperationType) String() string {
	switch o {
	case Get:
		return "get"
	case Put:
		return "put"
	case Delete:
		return "delete"
	default:
		return "unknown"
	}
}

func ParseOperationType(value string) (OperationType, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "get":
		return Get, nil
	case "put":
		return Put, nil
	case "delete":
		return Delete, nil
	default:
		return Get, fmt.Errorf("unknown operation type: %q", value)
	}
}
