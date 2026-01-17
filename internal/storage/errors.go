package storage

import "errors"

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrKeyLocked      = errors.New("key locked")
	ErrVectorNotFound = errors.New("vector not found")
	ErrUnsupported    = errors.New("operation not supported")
	ErrReservedValue  = errors.New("value is reserved")
	ErrCorruptData    = errors.New("corrupt data")
)
