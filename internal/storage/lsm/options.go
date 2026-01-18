package lsm

import (
	"errors"
	"time"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

const (
	DefaultMemtableMaxEntries       uint32        = 50_000
	DefaultMemtableMaxBytes         uint64        = 8 << 20
	DefaultSegmentMaxBytes          uint64        = 16 << 20
	DefaultBloomBitsPerKey          float64       = 10
	DefaultCompactionInterval       time.Duration = time.Minute
	DefaultCompactionMaxEntries     uint32        = 1_000_000
	DefaultCompactionMaxBytes       uint64        = 128 << 20
	DefaultCompactionRateLimitBytes uint64        = 4 << 20
	DefaultCompactionMaxSegments    uint32        = 4
	DefaultCompactionMaxInFlight    uint32        = 1
)

var ErrInvalidOptions = errors.New("invalid lsm options")

type LSMOptions struct {
	Dir                      string
	MemtableMaxEntries       uint32
	MemtableMaxBytes         uint64
	SegmentMaxBytes          uint64
	BloomBitsPerKey          float64
	CompactionInterval       time.Duration
	CompactionMaxEntries     uint32
	CompactionMaxBytes       uint64
	CompactionRateLimitBytes uint64
	CompactionMaxSegments    uint32
	CompactionMaxInFlight    uint32
}

func DefaultLSMOptions(dir string) LSMOptions {
	return LSMOptions{
		Dir:                      dir,
		MemtableMaxEntries:       DefaultMemtableMaxEntries,
		MemtableMaxBytes:         DefaultMemtableMaxBytes,
		SegmentMaxBytes:          DefaultSegmentMaxBytes,
		BloomBitsPerKey:          DefaultBloomBitsPerKey,
		CompactionInterval:       DefaultCompactionInterval,
		CompactionMaxEntries:     DefaultCompactionMaxEntries,
		CompactionMaxBytes:       DefaultCompactionMaxBytes,
		CompactionRateLimitBytes: DefaultCompactionRateLimitBytes,
		CompactionMaxSegments:    DefaultCompactionMaxSegments,
		CompactionMaxInFlight:    DefaultCompactionMaxInFlight,
	}
}

func (o *LSMOptions) Validate() error {
	if o == nil {
		return ErrInvalidOptions
	}
	if o.Dir == "" {
		return ErrInvalidOptions
	}
	if o.MemtableMaxEntries == 0 || o.MemtableMaxBytes == 0 {
		return ErrInvalidOptions
	}
	if o.SegmentMaxBytes == 0 {
		return ErrInvalidOptions
	}
	if o.BloomBitsPerKey <= 0 {
		return storage.ErrInvalidBloomParams
	}
	if o.CompactionInterval < 0 {
		return ErrInvalidOptions
	}
	if o.CompactionMaxEntries == 0 || o.CompactionMaxBytes == 0 {
		return ErrInvalidOptions
	}
	if o.CompactionMaxSegments == 0 || o.CompactionMaxInFlight == 0 {
		return ErrInvalidOptions
	}
	return nil
}
