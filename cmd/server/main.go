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
)

func main() {
	var (
		listen      string
		storageType string
		dataFile    string
		walFile     string
		partitions  int
	)

	flag.StringVar(&listen, "listen", ":8080", "HTTP listen address")
	flag.StringVar(&storageType, "storage", "memory", "storage backend: memory|partitioned|disk")
	flag.StringVar(&dataFile, "data", "bulletant.db", "disk storage file path")
	flag.StringVar(&walFile, "wal", "", "WAL file path (optional)")
	flag.IntVar(&partitions, "partitions", 8, "partition count for partitioned storage")
	flag.Parse()

	store, err := buildStorage(storageType, dataFile, partitions)
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

	database, err := db.Open(db.Options{
		Storage: store,
		WAL:     walLog,
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

func buildStorage(storageType string, dataFile string, partitions int) (storage.Storage, error) {
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
	default:
		return nil, errors.New("unknown storage type")
	}
}
