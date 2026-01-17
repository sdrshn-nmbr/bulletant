package client

import "errors"

var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrVectorNotFound   = errors.New("vector not found")
	ErrKeyLocked        = errors.New("key locked")
	ErrConflict         = errors.New("conflict")
	ErrUnsupported      = errors.New("operation not supported")
	ErrInvalidArgument  = errors.New("invalid argument")
	ErrRetryExhausted   = errors.New("retry attempts exhausted")
	ErrRequestFailed    = errors.New("request failed")
	ErrResponseTooLarge = errors.New("response too large")
)
