package storage

import "errors"

var (
	ErrKeyNotFound          = errors.New("key not found")
	ErrKeyLocked            = errors.New("key is locked")
	ErrVectorNotFound       = errors.New("vector not found")
	ErrVectorUnsupported    = errors.New("vector operations not supported")
	ErrUnsupportedOperation = errors.New("operation not supported")
	ErrCorruptData          = errors.New("corrupt data file")
	ErrUnsupportedVersion   = errors.New("unsupported disk format version")
)
