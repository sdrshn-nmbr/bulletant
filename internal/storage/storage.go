package storage

import (
	// "sync"
	"time"
)

type Key []byte
type Value []byte

type Entry struct {
	Key Key
	Value Value
	Timestamp time.Time
}

type Storage interface {
	Get(key Key) (Value, error)
	Put(key Key, value Value) error
	Delete (key Key) error
}
