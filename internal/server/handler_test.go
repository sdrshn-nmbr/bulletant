package server

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

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
