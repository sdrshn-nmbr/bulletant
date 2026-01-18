package main

import (
	"errors"
	"flag"
	"time"

	"github.com/sdrshn-nmbr/bulletant/internal/storage/lsm"
	"github.com/sdrshn-nmbr/bulletant/pkg/client"
)

type cliConfig struct {
	mode          string
	baseURL       string
	timeout       time.Duration
	keyEncoding   string
	valueEncoding string
	local         client.LocalOptions
}

func parseConfig(args []string) (cliConfig, []string, error) {
	var cfg cliConfig
	var storageType string
	var partitions uint
	var lsmMemtableEntries uint
	var lsmCompactionSegments uint
	var lsmCompactionInFlight uint
	var compactionMaxEntries uint
	var compactionMaxInFlight uint

	fs := flag.NewFlagSet("bulletant", flag.ContinueOnError)
	fs.StringVar(&cfg.mode, "mode", "local", "mode: local|http")
	fs.StringVar(&cfg.baseURL, "base-url", "http://localhost:8080", "http base url")
	fs.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "request timeout")
	fs.StringVar(&cfg.keyEncoding, "key-encoding", "utf-8", "key encoding: utf-8|base64")
	fs.StringVar(&cfg.valueEncoding, "value-encoding", "utf-8", "value encoding: utf-8|base64")

	fs.StringVar(&storageType, "storage-type", string(client.StorageMemory), "local storage type")
	fs.StringVar(&cfg.local.DataPath, "data", "bulletant.db", "local data path")
	fs.StringVar(&cfg.local.WALPath, "wal", "", "local wal path")
	fs.UintVar(&partitions, "partitions", 8, "local partition count")

	fs.StringVar(&cfg.local.LSMDir, "lsm-dir", "bulletant.lsm", "lsm directory")
	fs.UintVar(&lsmMemtableEntries, "lsm-memtable-max-entries", uint(lsm.DefaultMemtableMaxEntries), "lsm memtable max entries")
	fs.Uint64Var(&cfg.local.LSMMemtableMaxBytes, "lsm-memtable-max-bytes", lsm.DefaultMemtableMaxBytes, "lsm memtable max bytes")
	fs.Uint64Var(&cfg.local.LSMSegmentMaxBytes, "lsm-segment-max-bytes", lsm.DefaultSegmentMaxBytes, "lsm segment max bytes")
	fs.Float64Var(&cfg.local.LSMBloomBitsPerKey, "lsm-bloom-bits-per-key", lsm.DefaultBloomBitsPerKey, "lsm bloom bits per key")
	fs.UintVar(&lsmCompactionSegments, "lsm-compaction-max-segments", uint(lsm.DefaultCompactionMaxSegments), "lsm compaction max segments")
	fs.UintVar(&lsmCompactionInFlight, "lsm-compaction-max-in-flight", uint(lsm.DefaultCompactionMaxInFlight), "lsm compaction max in-flight")

	fs.DurationVar(&cfg.local.CompactionInterval, "compaction-interval", lsm.DefaultCompactionInterval, "compaction interval (0 disables)")
	fs.UintVar(&compactionMaxEntries, "compaction-max-entries", uint(lsm.DefaultCompactionMaxEntries), "compaction max entries")
	fs.Uint64Var(&cfg.local.CompactionMaxBytes, "compaction-max-bytes", lsm.DefaultCompactionMaxBytes, "compaction max bytes")
	fs.Uint64Var(&cfg.local.CompactionRateLimitBytes, "compaction-rate-limit-bytes", lsm.DefaultCompactionRateLimitBytes, "compaction rate limit bytes/sec")
	fs.UintVar(&compactionMaxInFlight, "compaction-max-in-flight", uint(lsm.DefaultCompactionMaxInFlight), "compaction max in-flight")

	if err := fs.Parse(args); err != nil {
		return cfg, nil, err
	}

	cfg.local.StorageType = client.StorageType(storageType)
	cfg.local.Partitions = uint32(partitions)
	cfg.local.LSMMemtableMaxEntries = uint32(lsmMemtableEntries)
	cfg.local.LSMCompactionMaxSegments = uint32(lsmCompactionSegments)
	cfg.local.LSMCompactionMaxInFlight = uint32(lsmCompactionInFlight)
	cfg.local.CompactionMaxEntries = uint32(compactionMaxEntries)
	cfg.local.CompactionMaxInFlight = uint32(compactionMaxInFlight)

	if cfg.mode != "local" && cfg.mode != "http" {
		return cfg, nil, errors.New("invalid mode")
	}
	if cfg.mode == "http" && cfg.baseURL == "" {
		return cfg, nil, errors.New("base-url is required for http mode")
	}

	return cfg, fs.Args(), nil
}
