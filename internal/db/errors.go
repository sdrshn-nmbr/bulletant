package db

import "errors"

var (
	ErrStorageRequired       = errors.New("storage is required")
	ErrWALBeginFailed        = errors.New("wal begin failed")
	ErrWALCommitFailed       = errors.New("wal commit failed")
	ErrWALAbortFailed        = errors.New("wal abort failed")
	ErrInvalidTxn            = errors.New("transaction is nil")
	ErrRecoveryFailed        = errors.New("wal recovery failed")
	ErrStorageCloseFail      = errors.New("storage close failed")
	ErrWALCloseFail          = errors.New("wal close failed")
	ErrCompactionUnsupported = errors.New("compaction unsupported by storage")
)
