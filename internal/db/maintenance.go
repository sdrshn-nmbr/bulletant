package db

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/sdrshn-nmbr/bulletant/internal/maintenance"
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

type CompactionOptions struct {
	Interval                time.Duration
	MaxEntries              uint32
	MaxBytes                uint64
	RateLimitBytesPerSecond uint64
	MaxInFlight             uint32
	TempPath                string
}

type SnapshotOptions struct {
	Path       string
	IncludeWAL bool
}

type SnapshotStats struct {
	Entries  uint32
	Bytes    uint64
	Path     string
	WALPath  string
	WALBytes uint64
	Duration time.Duration
}

type BackupOptions struct {
	Directory  string
	IncludeWAL bool
}

type BackupStats struct {
	Snapshot     SnapshotStats
	ManifestPath string
	Duration     time.Duration
}

type backupManifest struct {
	CreatedAt    time.Time     `json:"created_at"`
	Snapshot     SnapshotStats `json:"snapshot"`
	ManifestPath string        `json:"manifest_path"`
}

func startCompactionScheduler(
	store storage.Storage,
	opts CompactionOptions,
) (*maintenance.CompactionScheduler, context.CancelFunc, error) {
	if opts.Interval <= 0 {
		return nil, nil, nil
	}
	compacter, ok := store.(storage.Compacter)
	if !ok {
		return nil, nil, ErrCompactionUnsupported
	}
	if opts.MaxEntries == 0 || opts.MaxBytes == 0 {
		return nil, nil, storage.ErrInvalidCompactLimit
	}
	maxInFlight := int(opts.MaxInFlight)
	if maxInFlight <= 0 {
		maxInFlight = 1
	}

	config := maintenance.CompactionConfig{
		Compacter:               compacter,
		Interval:                opts.Interval,
		MaxEntries:              opts.MaxEntries,
		MaxBytes:                opts.MaxBytes,
		RateLimitBytesPerSecond: opts.RateLimitBytesPerSecond,
		MaxInFlight:             maxInFlight,
		TempPath:                opts.TempPath,
	}
	scheduler := maintenance.NewCompactionScheduler(config)
	ctx, cancel := context.WithCancel(context.Background())
	scheduler.Start(ctx)
	return scheduler, cancel, nil
}

func (d *DB) Snapshot(opts SnapshotOptions) (SnapshotStats, error) {
	start := time.Now()
	if opts.Path == "" {
		return SnapshotStats{}, storage.ErrInvalidPath
	}

	snapshotter, ok := d.storage.(storage.Snapshotter)
	if !ok {
		return SnapshotStats{}, storage.ErrSnapshotUnsupported
	}

	stats, err := snapshotter.Snapshot(storage.SnapshotOptions{Path: opts.Path})
	if err != nil {
		return SnapshotStats{}, err
	}

	result := SnapshotStats{
		Entries:  stats.Entries,
		Bytes:    stats.Bytes,
		Path:     stats.Path,
		Duration: time.Since(start),
	}

	if opts.IncludeWAL && d.wal != nil {
		walPath := walSnapshotPath(stats.Path)
		walBytes, err := d.wal.CopyTo(walPath)
		if err != nil {
			return result, err
		}
		result.WALPath = walPath
		result.WALBytes = walBytes
	}

	return result, nil
}

func (d *DB) Backup(opts BackupOptions) (BackupStats, error) {
	start := time.Now()
	if opts.Directory == "" {
		return BackupStats{}, storage.ErrInvalidPath
	}

	info, err := os.Stat(opts.Directory)
	if err != nil {
		return BackupStats{}, err
	}
	if !info.IsDir() {
		return BackupStats{}, storage.ErrInvalidPath
	}

	snapshotPath := filepath.Join(opts.Directory, "storage.snapshot")
	snapshotStats, err := d.Snapshot(SnapshotOptions{
		Path:       snapshotPath,
		IncludeWAL: opts.IncludeWAL,
	})
	if err != nil {
		return BackupStats{}, err
	}

	manifestPath := filepath.Join(opts.Directory, "manifest.json")
	if err := writeBackupManifest(manifestPath, snapshotStats); err != nil {
		return BackupStats{}, err
	}

	return BackupStats{
		Snapshot:     snapshotStats,
		ManifestPath: manifestPath,
		Duration:     time.Since(start),
	}, nil
}

func writeBackupManifest(path string, snapshot SnapshotStats) error {
	payload := backupManifest{
		CreatedAt:    time.Now(),
		Snapshot:     snapshot,
		ManifestPath: path,
	}

	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}

	return os.Rename(tempPath, path)
}

func walSnapshotPath(snapshotPath string) string {
	info, err := os.Stat(snapshotPath)
	if err == nil && info.IsDir() {
		return filepath.Join(snapshotPath, "wal.log")
	}
	return snapshotPath + ".wal"
}
