package client

import (
	"context"
	"net/http"
	"net/http/httptest"
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

