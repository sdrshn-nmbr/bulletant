package server

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdrshn-nmbr/bulletant/internal/billing"
	"github.com/sdrshn-nmbr/bulletant/internal/db"
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

func TestKVHandlers(t *testing.T) {
	store := storage.NewMemoryStorage()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("DB open failed: %v", err)
	}
	defer database.Close()

	handler := NewHandler(database)

	req := httptest.NewRequest(http.MethodPut, "/kv/foo", strings.NewReader(`{"value":"bar"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusNoContent {
		t.Fatalf("Expected 204, got %d", resp.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/kv/foo", nil)
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}

	var kv kvResponse
	if err := json.NewDecoder(resp.Body).Decode(&kv); err != nil {
		t.Fatalf("Decode response failed: %v", err)
	}

	value, err := base64.StdEncoding.DecodeString(kv.Value)
	if err != nil {
		t.Fatalf("Base64 decode failed: %v", err)
	}
	if string(value) != "bar" {
		t.Fatalf("Expected bar, got %s", string(value))
	}
}

func TestVectorHandlers(t *testing.T) {
	store := storage.NewMemoryStorage()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("DB open failed: %v", err)
	}
	defer database.Close()

	handler := NewHandler(database)

	req := httptest.NewRequest(http.MethodPost, "/vectors", strings.NewReader(`{"values":[0.1,0.2]}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusCreated {
		t.Fatalf("Expected 201, got %d", resp.Code)
	}

	var created vectorResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("Decode response failed: %v", err)
	}
	if created.ID == "" {
		t.Fatalf("Expected vector id")
	}

	req = httptest.NewRequest(http.MethodGet, "/vectors/"+created.ID, nil)
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}
}

func TestKVQueryHandlers(t *testing.T) {
	store := storage.NewMemoryStorage()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("DB open failed: %v", err)
	}
	defer database.Close()

	handler := NewHandler(database)
	encodedKey := base64.StdEncoding.EncodeToString([]byte("foo"))
	encodedValue := base64.StdEncoding.EncodeToString([]byte("bar"))

	body := `{"key":"` + encodedKey + `","value":"` + encodedValue + `","key_encoding":"base64","value_encoding":"base64"}`
	req := httptest.NewRequest(http.MethodPost, "/kv", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusNoContent {
		t.Fatalf("Expected 204, got %d", resp.Code)
	}

	query := "/kv?key=" + url.QueryEscape(encodedKey) + "&key_encoding=base64&value_encoding=base64"
	req = httptest.NewRequest(http.MethodGet, query, nil)
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}

	var kv kvResponse
	if err := json.NewDecoder(resp.Body).Decode(&kv); err != nil {
		t.Fatalf("Decode response failed: %v", err)
	}

	value, err := base64.StdEncoding.DecodeString(kv.Value)
	if err != nil {
		t.Fatalf("Base64 decode failed: %v", err)
	}
	if string(value) != "bar" {
		t.Fatalf("Expected bar, got %s", string(value))
	}
}

func TestScanHandler(t *testing.T) {
	store := storage.NewMemoryStorage()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("DB open failed: %v", err)
	}
	defer database.Close()

	if err := database.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := database.Put([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	handler := NewHandler(database)
	query := "/scan?limit=2&include_values=1&max_value_bytes=32&key_encoding=utf-8&value_encoding=base64"
	req := httptest.NewRequest(http.MethodGet, query, nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}

	var scan scanResponse
	if err := json.NewDecoder(resp.Body).Decode(&scan); err != nil {
		t.Fatalf("Decode response failed: %v", err)
	}
	if len(scan.Entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(scan.Entries))
	}
	if scan.Entries[0].Key != "alpha" {
		t.Fatalf("Expected alpha, got %s", scan.Entries[0].Key)
	}
}

func TestSnapshotHandler(t *testing.T) {
	store := storage.NewMemoryStorage()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("DB open failed: %v", err)
	}
	defer database.Close()

	if err := database.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	handler := NewHandler(database)
	snapshotPath := filepath.Join(t.TempDir(), "snapshot.bin")
	body := `{"path":"` + snapshotPath + `"}`
	req := httptest.NewRequest(http.MethodPost, "/maintenance/snapshot", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}

	if _, err := os.Stat(snapshotPath); err != nil {
		t.Fatalf("Snapshot not created: %v", err)
	}
}

func TestBackupHandler(t *testing.T) {
	store := storage.NewMemoryStorage()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("DB open failed: %v", err)
	}
	defer database.Close()

	if err := database.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	handler := NewHandler(database)
	backupDir := t.TempDir()
	body := `{"directory":"` + backupDir + `"}`
	req := httptest.NewRequest(http.MethodPost, "/maintenance/backup", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}

	if _, err := os.Stat(filepath.Join(backupDir, "manifest.json")); err != nil {
		t.Fatalf("Backup manifest not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(backupDir, "storage.snapshot")); err != nil {
		t.Fatalf("Backup snapshot not created: %v", err)
	}
}

func TestBillingHandlers(t *testing.T) {
	store := storage.NewMemoryStorage()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("DB open failed: %v", err)
	}
	defer database.Close()

	handler := NewHandler(database)

	createBody := `{"tenant":"tenant-a","id":"acct-1"}`
	req := httptest.NewRequest(http.MethodPost, "/billing/accounts", strings.NewReader(createBody))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusCreated {
		t.Fatalf("Expected 201, got %d", resp.Code)
	}

	creditBody := `{"tenant":"tenant-a","account_id":"acct-1","amount":1000}`
	req = httptest.NewRequest(http.MethodPost, "/billing/credits", strings.NewReader(creditBody))
	req.Header.Set("Content-Type", "application/json")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}

	usageBody := `{"id":"ev-1","tenant":"tenant-a","account_id":"acct-1","units":5,"unit_price":10}`
	req = httptest.NewRequest(http.MethodPost, "/billing/usage", strings.NewReader(usageBody))
	req.Header.Set("Content-Type", "application/json")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}

	var entry billing.LedgerEntry
	if err := json.NewDecoder(resp.Body).Decode(&entry); err != nil {
		t.Fatalf("Decode response failed: %v", err)
	}
	if entry.Cost != 50 {
		t.Fatalf("Expected cost 50, got %d", entry.Cost)
	}

	req = httptest.NewRequest(http.MethodGet, "/billing/accounts/acct-1/balance?tenant=tenant-a", nil)
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}

	var balance billing.Balance
	if err := json.NewDecoder(resp.Body).Decode(&balance); err != nil {
		t.Fatalf("Decode response failed: %v", err)
	}
	if balance.Amount != 950 {
		t.Fatalf("Expected balance 950, got %d", balance.Amount)
	}

	req = httptest.NewRequest(http.MethodGet, "/billing/usage?tenant=tenant-a&account_id=acct-1&limit=10", nil)
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}

	var list struct {
		Events     []billing.UsageEvent `json:"events"`
		NextCursor string               `json:"next_cursor"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("Decode response failed: %v", err)
	}
	if len(list.Events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(list.Events))
	}

	exportBody := `{"tenant":"tenant-a","limit":10}`
	req = httptest.NewRequest(http.MethodPost, "/billing/sync/export", strings.NewReader(exportBody))
	req.Header.Set("Content-Type", "application/json")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}

	scanner := bufio.NewScanner(resp.Body)
	exported := make([]billing.UsageEvent, 0)
	cursorSeen := false
	for scanner.Scan() {
		line := scanner.Bytes()
		var envelope struct {
			Type       string `json:"type"`
			NextCursor string `json:"next_cursor"`
		}
		if err := json.Unmarshal(line, &envelope); err != nil {
			t.Fatalf("Decode stream failed: %v", err)
		}
		if envelope.Type == "cursor" {
			cursorSeen = true
			continue
		}
		var event billing.UsageEvent
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("Decode event failed: %v", err)
		}
		exported = append(exported, event)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Scan export failed: %v", err)
	}
	if !cursorSeen {
		t.Fatalf("Expected cursor line in export")
	}
	if len(exported) != 1 {
		t.Fatalf("Expected 1 exported event, got %d", len(exported))
	}

	importPayload, err := json.Marshal(exported)
	if err != nil {
		t.Fatalf("Marshal import failed: %v", err)
	}
	req = httptest.NewRequest(http.MethodPost, "/billing/sync/import", strings.NewReader(string(importPayload)))
	req.Header.Set("Content-Type", "application/json")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}

	var stats billing.ImportStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("Decode import response failed: %v", err)
	}
	if stats.Skipped != 1 {
		t.Fatalf("Expected 1 skipped event, got %d", stats.Skipped)
	}
}
