package billing

import "errors"

var (
	ErrAccountExists       = errors.New("account already exists")
	ErrAccountNotFound     = errors.New("account not found")
	ErrInsufficientCredits = errors.New("insufficient credits")
	ErrDuplicateEvent      = errors.New("duplicate event")
	ErrPricePlanNotFound   = errors.New("price plan not found")
)
