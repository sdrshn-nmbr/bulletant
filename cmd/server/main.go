package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sdrshn-nmbr/bulletant/internal/db"
	wal "github.com/sdrshn-nmbr/bulletant/internal/log"
	"github.com/sdrshn-nmbr/bulletant/internal/server"
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/storage/lsm"
)

func main() {
	var (
		listen      string
		storageType string
		dataFile    string
		walFile     string
		partitions  int
		lsmDir      string
		lsmMemtableMaxEntries uint
		lsmMemtableMaxBytes   uint64
		lsmSegmentMaxBytes    uint64
		lsmBloomBitsPerKey    float64
		lsmCompactionMaxSegments uint
		lsmCompactionMaxInFlight uint
		compactionInterval      time.Duration
		compactionMaxEntries    uint
		compactionMaxBytes      uint64
		compactionRateLimit     uint64
		compactionMaxInFlight   uint
	)

	flag.StringVar(&listen, "listen", ":8080", "HTTP listen address")
	flag.StringVar(&storageType, "storage", "memory", "storage backend: memory|partitioned|disk|lsm")
	flag.StringVar(&dataFile, "data", "bulletant.db", "disk storage file path")
	flag.StringVar(&walFile, "wal", "", "WAL file path (optional)")
	flag.IntVar(&partitions, "partitions", 8, "partition count for partitioned storage")
	flag.StringVar(&lsmDir, "lsm-dir", "bulletant.lsm", "lsm storage directory")
	flag.UintVar(&lsmMemtableMaxEntries, "lsm-memtable-max-entries", uint(lsm.DefaultMemtableMaxEntries), "lsm memtable max entries")
	flag.Uint64Var(&lsmMemtableMaxBytes, "lsm-memtable-max-bytes", lsm.DefaultMemtableMaxBytes, "lsm memtable max bytes")
	flag.Uint64Var(&lsmSegmentMaxBytes, "lsm-segment-max-bytes", lsm.DefaultSegmentMaxBytes, "lsm segment max bytes")
	flag.Float64Var(&lsmBloomBitsPerKey, "lsm-bloom-bits-per-key", lsm.DefaultBloomBitsPerKey, "lsm bloom bits per key")
	flag.UintVar(&lsmCompactionMaxSegments, "lsm-compaction-max-segments", uint(lsm.DefaultCompactionMaxSegments), "lsm compaction max segments")
	flag.UintVar(&lsmCompactionMaxInFlight, "lsm-compaction-max-in-flight", uint(lsm.DefaultCompactionMaxInFlight), "lsm compaction max in-flight")
	flag.DurationVar(&compactionInterval, "compaction-interval", lsm.DefaultCompactionInterval, "compaction interval (0 to disable)")
	flag.UintVar(&compactionMaxEntries, "compaction-max-entries", uint(lsm.DefaultCompactionMaxEntries), "compaction max entries")
	flag.Uint64Var(&compactionMaxBytes, "compaction-max-bytes", lsm.DefaultCompactionMaxBytes, "compaction max bytes")
	flag.Uint64Var(&compactionRateLimit, "compaction-rate-limit-bytes", lsm.DefaultCompactionRateLimitBytes, "compaction rate limit bytes/sec")
	flag.UintVar(&compactionMaxInFlight, "compaction-max-in-flight", uint(lsm.DefaultCompactionMaxInFlight), "compaction max in-flight")
	flag.Parse()

	store, err := buildStorage(storageType, dataFile, partitions, lsmOptions{
		dir:                  lsmDir,
		memtableMaxEntries:   uint32(lsmMemtableMaxEntries),
		memtableMaxBytes:     lsmMemtableMaxBytes,
		segmentMaxBytes:      lsmSegmentMaxBytes,
		bloomBitsPerKey:      lsmBloomBitsPerKey,
		compactionMaxSegments: uint32(lsmCompactionMaxSegments),
		compactionMaxInFlight: uint32(lsmCompactionMaxInFlight),
	})
	if err != nil {
		log.Fatalf("Storage init failed: %v", err)
	}

	var walLog *wal.WAL
	if walFile != "" {
		walLog, err = wal.NewWAL(walFile)
		if err != nil {
			log.Fatalf("WAL init failed: %v", err)
		}
	}

	compactionOpts := db.CompactionOptions{
		Interval:                compactionInterval,
		MaxEntries:              uint32(compactionMaxEntries),
		MaxBytes:                compactionMaxBytes,
		RateLimitBytesPerSecond: compactionRateLimit,
		MaxInFlight:             uint32(compactionMaxInFlight),
	}
	if _, ok := store.(storage.Compacter); !ok {
		compactionOpts = db.CompactionOptions{}
	}

	database, err := db.Open(db.Options{
		Storage:    store,
		WAL:        walLog,
		Compaction: compactionOpts,
	})
	if err != nil {
		log.Fatalf("DB init failed: %v", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			log.Printf("DB close error: %v", err)
		}
	}()

	srv := &http.Server{
		Addr:              listen,
		Handler:           server.NewHandler(database),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("Bulletant listening on %s (%s storage)", listen, storageType)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}
}

type lsmOptions struct {
	dir                  string
	memtableMaxEntries   uint32
	memtableMaxBytes     uint64
	segmentMaxBytes      uint64
	bloomBitsPerKey      float64
	compactionMaxSegments uint32
	compactionMaxInFlight uint32
}

func buildStorage(storageType string, dataFile string, partitions int, opts lsmOptions) (storage.Storage, error) {
	switch strings.ToLower(storageType) {
	case "memory":
		return storage.NewMemoryStorage(), nil
	case "partitioned":
		if partitions <= 0 {
			partitions = 8
		}
		return storage.NewPartitionedStorage(partitions), nil
	case "disk":
		return storage.NewDiskStorage(dataFile)
	case "lsm":
		lsmOpts := lsm.DefaultLSMOptions(opts.dir)
		lsmOpts.MemtableMaxEntries = opts.memtableMaxEntries
		lsmOpts.MemtableMaxBytes = opts.memtableMaxBytes
		lsmOpts.SegmentMaxBytes = opts.segmentMaxBytes
		lsmOpts.BloomBitsPerKey = opts.bloomBitsPerKey
		lsmOpts.CompactionMaxSegments = opts.compactionMaxSegments
		lsmOpts.CompactionMaxInFlight = opts.compactionMaxInFlight
		return lsm.NewLSMStorage(lsmOpts)
	default:
		return nil, errors.New("unknown storage type")
	}
}
