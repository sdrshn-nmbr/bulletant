package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/sdrshn-nmbr/bulletant/internal/db"
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

const (
	maxBodyBytes       = 10 << 20
	maxScanEntries     = 10_000
	maxScanValueBytes  = 8 << 20
	maxScanPageEntries = 256
)

type handler struct {
	db *db.DB
}

type kvRequest struct {
	Value    string `json:"value"`
	Encoding string `json:"encoding,omitempty"`
}

type kvEnvelope struct {
	Key           string `json:"key"`
	Value         string `json:"value,omitempty"`
	KeyEncoding   string `json:"key_encoding,omitempty"`
	ValueEncoding string `json:"value_encoding,omitempty"`
}

type kvResponse struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	Encoding    string `json:"encoding"`
	KeyEncoding string `json:"key_encoding,omitempty"`
}

type txnRequest struct {
	Operations []txnOperation `json:"operations"`
}

type txnOperation struct {
	Type        string `json:"type"`
	Key         string `json:"key"`
	KeyEncoding string `json:"key_encoding,omitempty"`
	Value       string `json:"value,omitempty"`
	Encoding    string `json:"encoding,omitempty"`
}

type txnResponse struct {
	Status string `json:"status"`
}

type vectorRequest struct {
	Values   []float64              `json:"values"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type vectorResponse struct {
	ID       string                 `json:"id"`
	Values   []float64              `json:"values,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type scanEntryResponse struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type scanResponse struct {
	Entries       []scanEntryResponse `json:"entries"`
	NextCursor    string              `json:"next_cursor,omitempty"`
	KeyEncoding   string              `json:"key_encoding"`
	ValueEncoding string              `json:"value_encoding,omitempty"`
}

type scanStreamInfo struct {
	Type          string `json:"type"`
	NextCursor    string `json:"next_cursor,omitempty"`
	KeyEncoding   string `json:"key_encoding"`
	ValueEncoding string `json:"value_encoding,omitempty"`
}

type compactRequest struct {
	MaxEntries uint32 `json:"max_entries"`
	MaxBytes   uint64 `json:"max_bytes"`
	TempPath   string `json:"temp_path,omitempty"`
}

type compactResponse struct {
	EntriesTotal   uint32 `json:"entries_total"`
	EntriesWritten uint32 `json:"entries_written"`
	BytesBefore    uint64 `json:"bytes_before"`
	BytesAfter     uint64 `json:"bytes_after"`
}

type snapshotRequest struct {
	Path       string `json:"path"`
	IncludeWAL bool   `json:"include_wal,omitempty"`
}

type snapshotResponse struct {
	Entries    uint32 `json:"entries"`
	Bytes      uint64 `json:"bytes"`
	Path       string `json:"path"`
	WALPath    string `json:"wal_path,omitempty"`
	WALBytes   uint64 `json:"wal_bytes,omitempty"`
	DurationMs int64  `json:"duration_ms"`
}

type backupRequest struct {
	Directory  string `json:"directory"`
	IncludeWAL bool   `json:"include_wal,omitempty"`
}

type backupResponse struct {
	Snapshot     snapshotResponse `json:"snapshot"`
	ManifestPath string           `json:"manifest_path"`
	DurationMs   int64            `json:"duration_ms"`
}

func NewHandler(db *db.DB) http.Handler {
	h := &handler{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("/", h.handleRoot)
	mux.HandleFunc("/healthz", h.handleHealth)
	mux.HandleFunc("/kv", h.handleKVQuery)
	mux.HandleFunc("/kv/", h.handleKV)
	mux.HandleFunc("/scan", h.handleScan)
	mux.HandleFunc("/txn", h.handleTxn)
	mux.HandleFunc("/vectors", h.handleVectors)
	mux.HandleFunc("/vectors/", h.handleVectorByID)
	mux.HandleFunc("/maintenance/compact", h.handleCompact)
	mux.HandleFunc("/maintenance/snapshot", h.handleSnapshot)
	mux.HandleFunc("/maintenance/backup", h.handleBackup)
	return mux
}

func (h *handler) handleRoot(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"name":    "bulletant",
		"status":  "ok",
		"message": "ready",
	})
}

func (h *handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (h *handler) handleKV(w http.ResponseWriter, r *http.Request) {
	key, err := keyFromPath("/kv/", r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	switch r.Method {
	case http.MethodGet:
		value, err := h.db.Get([]byte(key))
		if err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		if r.URL.Query().Get("raw") == "1" {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(value)
			return
		}
		writeJSON(w, http.StatusOK, kvResponse{
			Key:         key,
			Value:       base64.StdEncoding.EncodeToString(value),
			Encoding:    "base64",
			KeyEncoding: "utf-8",
		})
	case http.MethodPut, http.MethodPost:
		value, err := readValue(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := h.db.Put([]byte(key), value); err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := h.db.Delete([]byte(key)); err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *handler) handleKVQuery(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		key, keyEncoding, err := readKeyQuery(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		value, err := h.db.Get(key)
		if err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		if r.URL.Query().Get("raw") == "1" {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(value)
			return
		}

		encoding := queryValueEncoding(r)
		encodedValue, err := encodeBytes(value, encoding)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		encodedKey, err := encodeBytes(key, keyEncoding)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, kvResponse{
			Key:         encodedKey,
			Value:       encodedValue,
			Encoding:    encoding,
			KeyEncoding: keyEncoding,
		})
	case http.MethodPut, http.MethodPost:
		var req kvEnvelope
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		key, err := decodeBytes(req.Key, req.KeyEncoding)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		value, err := decodeBytes(req.Value, req.ValueEncoding)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		if err := h.db.Put(key, value); err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		key, _, err := readKeyQuery(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := h.db.Delete(key); err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *handler) handleTxn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req txnRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if len(req.Operations) == 0 {
		writeError(w, http.StatusBadRequest, "no operations provided")
		return
	}

	txn := transaction.NewTransaction()
	for _, op := range req.Operations {
		opType, err := types.ParseOperationType(op.Type)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		key, err := decodeBytes(op.Key, op.KeyEncoding)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		switch opType {
		case types.Put:
			value, err := decodeBytes(op.Value, op.Encoding)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			txn.Put(types.Key(key), types.Value(value))
		case types.Delete:
			txn.Delete(types.Key(key))
		default:
			writeError(w, http.StatusBadRequest, "only put and delete are supported")
			return
		}
	}

	if err := h.db.ExecuteTransaction(txn); err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}

	writeJSON(w, http.StatusOK, txnResponse{
		Status: txn.Status.String(),
	})
}

func (h *handler) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	req, keyEncoding, valueEncoding, stream, err := readScanQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if stream {
		h.streamScan(w, r, req, keyEncoding, valueEncoding)
		return
	}

	result, err := h.db.Scan(req)
	if err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}

	entries, err := encodeScanEntries(
		result.Entries,
		keyEncoding,
		valueEncoding,
		req.IncludeValues,
	)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	nextCursor, err := encodeBytes([]byte(result.NextCursor), keyEncoding)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, scanResponse{
		Entries:       entries,
		NextCursor:    nextCursor,
		KeyEncoding:   keyEncoding,
		ValueEncoding: valueEncoding,
	})
}

func (h *handler) handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req compactRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.MaxEntries == 0 || req.MaxBytes == 0 {
		writeError(w, http.StatusBadRequest, "max_entries and max_bytes are required")
		return
	}

	stats, err := h.db.Compact(storage.CompactOptions{
		MaxEntries: req.MaxEntries,
		MaxBytes:   req.MaxBytes,
		TempPath:   req.TempPath,
		Context:    r.Context(),
	})
	if err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}

	writeJSON(w, http.StatusOK, compactResponse{
		EntriesTotal:   stats.EntriesTotal,
		EntriesWritten: stats.EntriesWritten,
		BytesBefore:    stats.BytesBefore,
		BytesAfter:     stats.BytesAfter,
	})
}

func (h *handler) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req snapshotRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validatePathInput(req.Path); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	stats, err := h.db.Snapshot(db.SnapshotOptions{
		Path:       req.Path,
		IncludeWAL: req.IncludeWAL,
	})
	if err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}

	writeJSON(w, http.StatusOK, snapshotResponse{
		Entries:    stats.Entries,
		Bytes:      stats.Bytes,
		Path:       stats.Path,
		WALPath:    stats.WALPath,
		WALBytes:   stats.WALBytes,
		DurationMs: stats.Duration.Milliseconds(),
	})
}

func (h *handler) handleBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req backupRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validatePathInput(req.Directory); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	info, err := os.Stat(req.Directory)
	if err != nil || !info.IsDir() {
		writeError(w, http.StatusBadRequest, "directory does not exist")
		return
	}

	stats, err := h.db.Backup(db.BackupOptions{
		Directory:  req.Directory,
		IncludeWAL: req.IncludeWAL,
	})
	if err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}

	writeJSON(w, http.StatusOK, backupResponse{
		Snapshot: snapshotResponse{
			Entries:    stats.Snapshot.Entries,
			Bytes:      stats.Snapshot.Bytes,
			Path:       stats.Snapshot.Path,
			WALPath:    stats.Snapshot.WALPath,
			WALBytes:   stats.Snapshot.WALBytes,
			DurationMs: stats.Snapshot.Duration.Milliseconds(),
		},
		ManifestPath: stats.ManifestPath,
		DurationMs:   stats.Duration.Milliseconds(),
	})
}

func (h *handler) handleVectors(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req vectorRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Values) == 0 {
		writeError(w, http.StatusBadRequest, "values are required")
		return
	}

	id, err := h.db.AddVector(req.Values, req.Metadata)
	if err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, vectorResponse{
		ID: id,
	})
}

func (h *handler) handleVectorByID(w http.ResponseWriter, r *http.Request) {
	id, err := keyFromPath("/vectors/", r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	switch r.Method {
	case http.MethodGet:
		vector, err := h.db.GetVector(id)
		if err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		writeJSON(w, http.StatusOK, vectorResponse{
			ID:       vector.ID,
			Values:   vector.Values,
			Metadata: vector.Metadata,
		})
	case http.MethodDelete:
		if err := h.db.DeleteVector(id); err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func readValue(r *http.Request) ([]byte, error) {
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/json") {
		var req kvRequest
		if err := decodeJSON(r, &req); err != nil {
			return nil, err
		}
		return decodeBytes(req.Value, req.Encoding)
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		return nil, err
	}
	return body, nil
}

func decodeBytes(value string, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "utf-8", "text":
		return []byte(value), nil
	case "base64", "b64":
		return base64.StdEncoding.DecodeString(value)
	default:
		return nil, errors.New("unsupported encoding")
	}
}

func encodeBytes(value []byte, encoding string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "utf-8", "text":
		if !utf8.Valid(value) {
			return "", errors.New("value is not valid utf-8")
		}
		return string(value), nil
	case "base64", "b64":
		return base64.StdEncoding.EncodeToString(value), nil
	default:
		return "", errors.New("unsupported encoding")
	}
}

func readKeyQuery(r *http.Request) ([]byte, string, error) {
	query := r.URL.Query()
	keyRaw := query.Get("key")
	if keyRaw == "" {
		return nil, "", errors.New("missing key")
	}

	keyEncoding := queryKeyEncoding(query)
	key, err := decodeBytes(keyRaw, keyEncoding)
	if err != nil {
		return nil, "", err
	}

	return key, keyEncoding, nil
}

func queryKeyEncoding(query url.Values) string {
	encoding := strings.ToLower(strings.TrimSpace(query.Get("key_encoding")))
	if encoding == "" {
		return "utf-8"
	}
	return encoding
}

func queryValueEncoding(r *http.Request) string {
	encoding := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("value_encoding")))
	if encoding == "" {
		return "base64"
	}
	return encoding
}

func readScanQuery(
	r *http.Request,
) (storage.ScanRequest, string, string, bool, error) {
	query := r.URL.Query()
	limitRaw := query.Get("limit")
	if limitRaw == "" {
		return storage.ScanRequest{}, "", "", false, errors.New("limit is required")
	}

	limit, err := parseUint32(limitRaw, maxScanEntries)
	if err != nil {
		return storage.ScanRequest{}, "", "", false, err
	}

	stream, err := parseBool(query.Get("stream"))
	if err != nil {
		return storage.ScanRequest{}, "", "", false, err
	}

	includeValues, err := parseBool(query.Get("include_values"))
	if err != nil {
		return storage.ScanRequest{}, "", "", false, err
	}

	keyEncoding := queryEncoding(query, "key_encoding", "base64")
	valueEncoding := queryEncoding(query, "value_encoding", "base64")

	cursor, err := decodeOptionalKey(query.Get("cursor"), keyEncoding)
	if err != nil {
		return storage.ScanRequest{}, "", "", false, err
	}

	prefix, err := decodeOptionalKey(query.Get("prefix"), keyEncoding)
	if err != nil {
		return storage.ScanRequest{}, "", "", false, err
	}

	maxValueBytes := uint32(0)
	if includeValues {
		maxValueBytes, err = parseValueLimit(query.Get("max_value_bytes"))
		if err != nil {
			return storage.ScanRequest{}, "", "", false, err
		}
	}

	req := storage.ScanRequest{
		Cursor:        types.Key(cursor),
		Prefix:        types.Key(prefix),
		Limit:         limit,
		IncludeValues: includeValues,
		MaxValueBytes: maxValueBytes,
	}
	return req, keyEncoding, valueEncoding, stream, nil
}

func decodeOptionalKey(value string, encoding string) ([]byte, error) {
	if value == "" {
		return nil, nil
	}
	return decodeBytes(value, encoding)
}

func parseValueLimit(raw string) (uint32, error) {
	if raw == "" {
		return maxScanValueBytes, nil
	}
	return parseUint32(raw, maxScanValueBytes)
}

func queryEncoding(query url.Values, key string, fallback string) string {
	encoding := strings.ToLower(strings.TrimSpace(query.Get(key)))
	if encoding == "" {
		return fallback
	}
	return encoding
}

func parseUint32(value string, max uint32) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, errors.New("invalid integer")
	}
	if parsed == 0 {
		return 0, errors.New("value must be greater than zero")
	}
	if parsed > uint64(max) {
		return 0, errors.New("value exceeds limit")
	}
	return uint32(parsed), nil
}

func parseBool(value string) (bool, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return false, nil
	}
	if value == "1" || value == "true" {
		return true, nil
	}
	if value == "0" || value == "false" {
		return false, nil
	}
	return false, errors.New("invalid boolean")
}

func encodeScanEntries(
	entries []storage.ScanEntry,
	keyEncoding string,
	valueEncoding string,
	includeValues bool,
) ([]scanEntryResponse, error) {
	resp := make([]scanEntryResponse, 0, len(entries))
	for _, entry := range entries {
		encoded, err := encodeScanEntry(
			entry,
			keyEncoding,
			valueEncoding,
			includeValues,
		)
		if err != nil {
			return nil, err
		}
		resp = append(resp, encoded)
	}
	return resp, nil
}

func encodeScanEntry(
	entry storage.ScanEntry,
	keyEncoding string,
	valueEncoding string,
	includeValues bool,
) (scanEntryResponse, error) {
	encodedKey, err := encodeBytes([]byte(entry.Key), keyEncoding)
	if err != nil {
		return scanEntryResponse{}, err
	}
	if !includeValues {
		return scanEntryResponse{Key: encodedKey}, nil
	}

	encodedValue, err := encodeBytes(entry.Value, valueEncoding)
	if err != nil {
		return scanEntryResponse{}, err
	}

	return scanEntryResponse{
		Key:   encodedKey,
		Value: encodedValue,
	}, nil
}

func (h *handler) streamScan(
	w http.ResponseWriter,
	r *http.Request,
	req storage.ScanRequest,
	keyEncoding string,
	valueEncoding string,
) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)

	flusher, _ := w.(http.Flusher)
	cursor := req.Cursor
	remaining := req.Limit

	for remaining > 0 {
		if err := r.Context().Err(); err != nil {
			return
		}

		pageLimit := remaining
		if pageLimit > maxScanPageEntries {
			pageLimit = maxScanPageEntries
		}

		pageReq := req
		pageReq.Limit = pageLimit
		pageReq.Cursor = cursor

		result, err := h.db.Scan(pageReq)
		if err != nil {
			writeStreamError(encoder, err)
			return
		}

		if len(result.Entries) == 0 {
			cursor = result.NextCursor
			break
		}

		for _, entry := range result.Entries {
			resp, err := encodeScanEntry(
				entry,
				keyEncoding,
				valueEncoding,
				req.IncludeValues,
			)
			if err != nil {
				writeStreamError(encoder, err)
				return
			}
			if err := encoder.Encode(resp); err != nil {
				return
			}
		}

		if flusher != nil {
			flusher.Flush()
		}

		remaining -= uint32(len(result.Entries))
		if result.NextCursor == "" {
			cursor = ""
			break
		}
		cursor = result.NextCursor
	}

	nextCursor, err := encodeBytes([]byte(cursor), keyEncoding)
	if err != nil {
		writeStreamError(encoder, err)
		return
	}

	info := scanStreamInfo{
		Type:          "cursor",
		NextCursor:    nextCursor,
		KeyEncoding:   keyEncoding,
		ValueEncoding: valueEncoding,
	}

	_ = encoder.Encode(info)
	if flusher != nil {
		flusher.Flush()
	}
}

func writeStreamError(encoder *json.Encoder, err error) {
	_ = encoder.Encode(map[string]string{
		"type":  "error",
		"error": err.Error(),
	})
}

func decodeJSON(r *http.Request, dest any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	decoder.DisallowUnknownFields()
	return decoder.Decode(dest)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func statusForError(err error) int {
	switch {
	case errors.Is(err, storage.ErrKeyNotFound),
		errors.Is(err, storage.ErrVectorNotFound):
		return http.StatusNotFound
	case errors.Is(err, storage.ErrKeyLocked):
		return http.StatusConflict
	case errors.Is(err, storage.ErrReservedValue):
		return http.StatusConflict
	case errors.Is(err, storage.ErrInvalidScanLimit),
		errors.Is(err, storage.ErrInvalidValueLimit),
		errors.Is(err, storage.ErrInvalidCompactLimit),
		errors.Is(err, storage.ErrInvalidPath),
		errors.Is(err, storage.ErrInvalidBloomParams):
		return http.StatusBadRequest
	case errors.Is(err, storage.ErrValueTooLarge):
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, storage.ErrCompactLimitExceeded):
		return http.StatusConflict
	case errors.Is(err, storage.ErrUnsupported),
		errors.Is(err, storage.ErrSnapshotUnsupported):
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}

func validatePathInput(path string) error {
	if strings.TrimSpace(path) == "" {
		return storage.ErrInvalidPath
	}
	if strings.Contains(path, "..") {
		return storage.ErrInvalidPath
	}
	return nil
}

func keyFromPath(prefix string, path string) (string, error) {
	if !strings.HasPrefix(path, prefix) {
		return "", errors.New("invalid path")
	}
	raw := strings.TrimPrefix(path, prefix)
	if raw == "" {
		return "", errors.New("missing key")
	}
	decoded, err := url.PathUnescape(raw)
	if err != nil {
		return "", err
	}
	return decoded, nil
}
