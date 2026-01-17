package client

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
