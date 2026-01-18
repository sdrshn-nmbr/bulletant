package storage

import "context"

type RateLimiter interface {
	WaitN(ctx context.Context, n int64) error
}

type CompactOptions struct {
	MaxEntries  uint32
	MaxBytes    uint64
	TempPath    string
	Context     context.Context
	RateLimiter RateLimiter
}

type CompactStats struct {
	EntriesTotal   uint32
	EntriesWritten uint32
	BytesBefore    uint64
	BytesAfter     uint64
}

type Compacter interface {
	Compact(opts CompactOptions) (CompactStats, error)
}
