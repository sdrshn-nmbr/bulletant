package storage

type CompactOptions struct {
	MaxEntries uint32
	MaxBytes   uint64
	TempPath   string
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
