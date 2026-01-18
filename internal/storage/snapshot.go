package storage

type SnapshotOptions struct {
	Path     string
	TempPath string
}

type SnapshotStats struct {
	Entries uint32
	Bytes   uint64
	Path    string
}

type Snapshotter interface {
	Snapshot(opts SnapshotOptions) (SnapshotStats, error)
}
