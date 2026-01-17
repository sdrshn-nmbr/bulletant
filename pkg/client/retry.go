package client

import (
	"errors"
	"time"
)

type RetryPolicy struct {
	MaxAttempts      uint32
	BaseDelay        time.Duration
	MaxDelay         time.Duration
	Jitter           time.Duration
	RetryStatusCodes []int
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:      4,
		BaseDelay:        50 * time.Millisecond,
		MaxDelay:         2 * time.Second,
		Jitter:           25 * time.Millisecond,
		RetryStatusCodes: []int{429, 500, 502, 503, 504},
	}
}

func (p RetryPolicy) Validate() error {
	if p.MaxAttempts == 0 {
		return ErrInvalidArgument
	}
	if p.BaseDelay <= 0 {
		return ErrInvalidArgument
	}
	if p.MaxDelay < p.BaseDelay {
		return ErrInvalidArgument
	}
	if len(p.RetryStatusCodes) == 0 {
		return ErrInvalidArgument
	}
	if p.Jitter < 0 {
		return ErrInvalidArgument
	}
	for _, code := range p.RetryStatusCodes {
		if code < 100 || code > 599 {
			return errors.New("invalid status code")
		}
	}
	return nil
}
