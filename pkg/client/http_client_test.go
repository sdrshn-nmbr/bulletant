package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sdrshn-nmbr/bulletant/internal/db"
	"github.com/sdrshn-nmbr/bulletant/internal/server"
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

func TestHTTPClientKVAndScan(t *testing.T) {
	store := storage.NewMemoryStorage()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("DB open failed: %v", err)
	}
	defer database.Close()

	ts := httptest.NewServer(server.NewHandler(database))
	defer ts.Close()

	httpClient, err := NewHTTPClient(HTTPOptions{
		BaseURL:          ts.URL,
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		RetryPolicy:      DefaultRetryPolicy(),
		KeyEncoding:      "base64",
		ValueEncoding:    "base64",
		MaxResponseBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	ctx := context.Background()
	if err := httpClient.Put(ctx, []byte("alpha"), []byte("one"), RequestOptions{}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	value, err := httpClient.Get(ctx, []byte("alpha"), RequestOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "one" {
		t.Fatalf("Expected one, got %s", string(value))
	}

	_, err = httpClient.Scan(ctx, ScanRequest{
		Limit:         1,
		IncludeValues: true,
		MaxValueBytes: 16,
	}, RequestOptions{})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
}

func TestHTTPClientScanStream(t *testing.T) {
	store := storage.NewMemoryStorage()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("DB open failed: %v", err)
	}
	defer database.Close()

	_ = database.Put([]byte("alpha"), []byte("one"))
	_ = database.Put([]byte("beta"), []byte("two"))

	ts := httptest.NewServer(server.NewHandler(database))
	defer ts.Close()

	httpClient, err := NewHTTPClient(HTTPOptions{
		BaseURL:          ts.URL,
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		RetryPolicy:      DefaultRetryPolicy(),
		KeyEncoding:      "base64",
		ValueEncoding:    "base64",
		MaxResponseBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	var seen []ScanEntry
	ctx := context.Background()
	_, err = httpClient.ScanStream(ctx, ScanRequest{
		Limit:         10,
		IncludeValues: true,
		MaxValueBytes: 16,
	}, RequestOptions{}, func(entry ScanEntry) error {
		seen = append(seen, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanStream failed: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(seen))
	}
}

func TestHTTPClientSnapshotBackup(t *testing.T) {
	store := storage.NewMemoryStorage()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("DB open failed: %v", err)
	}
	defer database.Close()

	_ = database.Put([]byte("alpha"), []byte("one"))

	ts := httptest.NewServer(server.NewHandler(database))
	defer ts.Close()

	httpClient, err := NewHTTPClient(HTTPOptions{
		BaseURL:          ts.URL,
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		RetryPolicy:      DefaultRetryPolicy(),
		KeyEncoding:      "base64",
		ValueEncoding:    "base64",
		MaxResponseBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	snapshotPath := filepath.Join(t.TempDir(), "snapshot.bin")
	snapshotStats, err := httpClient.Snapshot(context.Background(), SnapshotOptions{
		Path: snapshotPath,
	}, RequestOptions{})
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if snapshotStats.Path == "" {
		t.Fatalf("expected snapshot path")
	}
	if _, err := os.Stat(snapshotPath); err != nil {
		t.Fatalf("snapshot not created: %v", err)
	}

	backupDir := t.TempDir()
	backupStats, err := httpClient.Backup(context.Background(), BackupOptions{
		Directory: backupDir,
	}, RequestOptions{})
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}
	if backupStats.ManifestPath == "" {
		t.Fatalf("expected manifest path")
	}
	if _, err := os.Stat(filepath.Join(backupDir, "manifest.json")); err != nil {
		t.Fatalf("manifest not created: %v", err)
	}
}
