package client

import "errors"

var (
	ErrKeyNotFound         = errors.New("key not found")
	ErrVectorNotFound      = errors.New("vector not found")
	ErrKeyLocked           = errors.New("key locked")
	ErrConflict            = errors.New("conflict")
	ErrUnsupported         = errors.New("operation not supported")
	ErrInvalidArgument     = errors.New("invalid argument")
	ErrRetryExhausted      = errors.New("retry attempts exhausted")
	ErrRequestFailed       = errors.New("request failed")
	ErrResponseTooLarge    = errors.New("response too large")
	ErrAccountExists       = errors.New("account already exists")
	ErrAccountNotFound     = errors.New("account not found")
	ErrInsufficientCredits = errors.New("insufficient credits")
	ErrDuplicateEvent      = errors.New("duplicate event")
	ErrPricePlanNotFound   = errors.New("price plan not found")
)
