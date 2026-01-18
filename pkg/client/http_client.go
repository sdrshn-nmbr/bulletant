package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type HTTPOptions struct {
	BaseURL          string
	HTTPClient       *http.Client
	RetryPolicy      RetryPolicy
	KeyEncoding      string
	ValueEncoding    string
	UserAgent        string
	MaxResponseBytes uint64
}

type RequestOptions struct {
	DisableRetry  bool
	KeyEncoding   string
	ValueEncoding string
}

type HTTPClient struct {
	baseURL          *url.URL
	httpClient       *http.Client
	retryPolicy      RetryPolicy
	retryStatus      map[int]struct{}
	keyEncoding      string
	valueEncoding    string
	userAgent        string
	maxResponseBytes uint64
	rngMu            sync.Mutex
	rng              *rand.Rand
}

type HTTPError struct {
	StatusCode int
	Message    string
}

func (e *HTTPError) Error() string {
	if e.Message == "" {
		return "request failed"
	}
	return e.Message
}

func NewHTTPClient(opts HTTPOptions) (*HTTPClient, error) {
	if opts.BaseURL == "" {
		return nil, ErrInvalidArgument
	}
	if opts.HTTPClient == nil {
		return nil, ErrInvalidArgument
	}
	if opts.KeyEncoding == "" || opts.ValueEncoding == "" {
		return nil, ErrInvalidArgument
	}
	if opts.MaxResponseBytes == 0 {
		return nil, ErrInvalidArgument
	}
	if err := opts.RetryPolicy.Validate(); err != nil {
		return nil, err
	}

	parsed, err := url.Parse(opts.BaseURL)
	if err != nil {
		return nil, err
	}

	retryStatus := make(map[int]struct{}, len(opts.RetryPolicy.RetryStatusCodes))
	for _, code := range opts.RetryPolicy.RetryStatusCodes {
		retryStatus[code] = struct{}{}
	}

	return &HTTPClient{
		baseURL:          parsed,
		httpClient:       opts.HTTPClient,
		retryPolicy:      opts.RetryPolicy,
		retryStatus:      retryStatus,
		keyEncoding:      opts.KeyEncoding,
		valueEncoding:    opts.ValueEncoding,
		userAgent:        opts.UserAgent,
		maxResponseBytes: opts.MaxResponseBytes,
		rng:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

func (c *HTTPClient) Get(
	ctx context.Context,
	key []byte,
	opts RequestOptions,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	keyEncoding, valueEncoding := c.resolveEncodings(opts)
	encodedKey, err := encodeBytes(key, keyEncoding)
	if err != nil {
		return nil, err
	}

	query := url.Values{}
	query.Set("key", encodedKey)
	query.Set("key_encoding", keyEncoding)
	query.Set("value_encoding", valueEncoding)

	requestURL := c.buildURL("/kv", query)
	var result []byte

	err = c.doRequest(ctx, requestSpec{
		method: http.MethodGet,
		url:    requestURL,
	}, opts, func(resp *http.Response) error {
		decoded, err := c.decodeKVResponse(resp)
		if err != nil {
			return err
		}
		result = decoded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *HTTPClient) Put(
	ctx context.Context,
	key []byte,
	value []byte,
	opts RequestOptions,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	keyEncoding, valueEncoding := c.resolveEncodings(opts)
	payload, err := c.encodeKVEnvelope(key, value, keyEncoding, valueEncoding)
	if err != nil {
		return err
	}

	requestURL := c.buildURL("/kv", nil)
	return c.doRequest(ctx, requestSpec{
		method: http.MethodPost,
		url:    requestURL,
		body:   payload,
		headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, opts, drainResponse)
}

func (c *HTTPClient) Delete(
	ctx context.Context,
	key []byte,
	opts RequestOptions,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	keyEncoding, _ := c.resolveEncodings(opts)
	encodedKey, err := encodeBytes(key, keyEncoding)
	if err != nil {
		return err
	}

	query := url.Values{}
	query.Set("key", encodedKey)
	query.Set("key_encoding", keyEncoding)

	requestURL := c.buildURL("/kv", query)
	return c.doRequest(ctx, requestSpec{
		method: http.MethodDelete,
		url:    requestURL,
	}, opts, drainResponse)
}

func (c *HTTPClient) ExecuteTransaction(
	ctx context.Context,
	ops []TransactionOperation,
	opts RequestOptions,
) (TransactionStatus, error) {
	if err := ctx.Err(); err != nil {
		return TransactionAborted, err
	}
	if len(ops) == 0 {
		return TransactionAborted, ErrInvalidArgument
	}

	keyEncoding, valueEncoding := c.resolveEncodings(opts)
	payload, err := c.encodeTxnRequest(ops, keyEncoding, valueEncoding)
	if err != nil {
		return TransactionAborted, err
	}

	requestURL := c.buildURL("/txn", nil)
	var status TransactionStatus

	err = c.doRequest(ctx, requestSpec{
		method: http.MethodPost,
		url:    requestURL,
		body:   payload,
		headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, opts, func(resp *http.Response) error {
		return c.decodeTxnResponse(resp, &status)
	})
	if err != nil {
		return TransactionAborted, err
	}
	return status, nil
}

func (c *HTTPClient) Scan(
	ctx context.Context,
	req ScanRequest,
	opts RequestOptions,
) (ScanResult, error) {
	if err := ctx.Err(); err != nil {
		return ScanResult{}, err
	}
	if req.Limit == 0 {
		return ScanResult{}, ErrInvalidArgument
	}
	if req.IncludeValues && req.MaxValueBytes == 0 {
		return ScanResult{}, ErrInvalidArgument
	}

	keyEncoding, valueEncoding := c.resolveEncodings(opts)
	query, err := c.encodeScanQuery(req, keyEncoding, valueEncoding)
	if err != nil {
		return ScanResult{}, err
	}

	requestURL := c.buildURL("/scan", query)
	var result ScanResult
	err = c.doRequest(ctx, requestSpec{
		method: http.MethodGet,
		url:    requestURL,
	}, opts, func(resp *http.Response) error {
		return c.decodeScanResponse(resp, &result, req.IncludeValues)
	})
	if err != nil {
		return ScanResult{}, err
	}
	return result, nil
}

func (c *HTTPClient) ScanStream(
	ctx context.Context,
	req ScanRequest,
	opts RequestOptions,
	handler func(ScanEntry) error,
) (ScanResult, error) {
	if err := ctx.Err(); err != nil {
		return ScanResult{}, err
	}
	if req.Limit == 0 {
		return ScanResult{}, ErrInvalidArgument
	}
	if req.IncludeValues && req.MaxValueBytes == 0 {
		return ScanResult{}, ErrInvalidArgument
	}
	if handler == nil {
		return ScanResult{}, ErrInvalidArgument
	}

	keyEncoding, valueEncoding := c.resolveEncodings(opts)
	query, err := c.encodeScanQuery(req, keyEncoding, valueEncoding)
	if err != nil {
		return ScanResult{}, err
	}
	query.Set("stream", "1")

	requestURL := c.buildURL("/scan", query)
	streamOpts := opts
	streamOpts.DisableRetry = true
	var result ScanResult

	err = c.doRequest(ctx, requestSpec{
		method: http.MethodGet,
		url:    requestURL,
	}, streamOpts, func(resp *http.Response) error {
		return c.decodeScanStream(
			resp,
			handler,
			&result,
			req.IncludeValues,
			keyEncoding,
			valueEncoding,
		)
	})
	if err != nil {
		return ScanResult{}, err
	}
	return result, nil
}

func (c *HTTPClient) AddVector(
	ctx context.Context,
	values []float64,
	metadata map[string]interface{},
	opts RequestOptions,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(values) == 0 {
		return "", ErrInvalidArgument
	}

	payload, err := json.Marshal(map[string]any{
		"values":   values,
		"metadata": metadata,
	})
	if err != nil {
		return "", err
	}

	requestURL := c.buildURL("/vectors", nil)
	var id string

	err = c.doRequest(ctx, requestSpec{
		method: http.MethodPost,
		url:    requestURL,
		body:   payload,
		headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, opts, func(resp *http.Response) error {
		return c.decodeVectorCreate(resp, &id)
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func (c *HTTPClient) GetVector(
	ctx context.Context,
	id string,
	opts RequestOptions,
) (*Vector, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id == "" {
		return nil, ErrInvalidArgument
	}

	requestURL := c.buildURL("/vectors/"+url.PathEscape(id), nil)
	var vector Vector

	err := c.doRequest(ctx, requestSpec{
		method: http.MethodGet,
		url:    requestURL,
	}, opts, func(resp *http.Response) error {
		return c.decodeVector(resp, &vector)
	})
	if err != nil {
		return nil, err
	}
	return &vector, nil
}

func (c *HTTPClient) DeleteVector(
	ctx context.Context,
	id string,
	opts RequestOptions,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == "" {
		return ErrInvalidArgument
	}

	requestURL := c.buildURL("/vectors/"+url.PathEscape(id), nil)
	return c.doRequest(ctx, requestSpec{
		method: http.MethodDelete,
		url:    requestURL,
	}, opts, drainResponse)
}

func (c *HTTPClient) Compact(
	ctx context.Context,
	opts CompactOptions,
	reqOpts RequestOptions,
) (CompactStats, error) {
	if err := ctx.Err(); err != nil {
		return CompactStats{}, err
	}
	if opts.MaxEntries == 0 || opts.MaxBytes == 0 {
		return CompactStats{}, ErrInvalidArgument
	}

	payload, err := json.Marshal(map[string]any{
		"max_entries": opts.MaxEntries,
		"max_bytes":   opts.MaxBytes,
		"temp_path":   opts.TempPath,
	})
	if err != nil {
		return CompactStats{}, err
	}

	requestURL := c.buildURL("/maintenance/compact", nil)
	var stats CompactStats

	err = c.doRequest(ctx, requestSpec{
		method: http.MethodPost,
		url:    requestURL,
		body:   payload,
		headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, reqOpts, func(resp *http.Response) error {
		return c.decodeCompact(resp, &stats)
	})
	if err != nil {
		return CompactStats{}, err
	}
	return stats, nil
}

func (c *HTTPClient) Snapshot(
	ctx context.Context,
	opts SnapshotOptions,
	reqOpts RequestOptions,
) (SnapshotStats, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotStats{}, err
	}
	if opts.Path == "" {
		return SnapshotStats{}, ErrInvalidArgument
	}

	payload, err := json.Marshal(map[string]any{
		"path":        opts.Path,
		"include_wal": opts.IncludeWAL,
	})
	if err != nil {
		return SnapshotStats{}, err
	}

	requestURL := c.buildURL("/maintenance/snapshot", nil)
	var stats SnapshotStats

	err = c.doRequest(ctx, requestSpec{
		method: http.MethodPost,
		url:    requestURL,
		body:   payload,
		headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, reqOpts, func(resp *http.Response) error {
		return c.decodeSnapshot(resp, &stats)
	})
	if err != nil {
		return SnapshotStats{}, err
	}
	return stats, nil
}

func (c *HTTPClient) Backup(
	ctx context.Context,
	opts BackupOptions,
	reqOpts RequestOptions,
) (BackupStats, error) {
	if err := ctx.Err(); err != nil {
		return BackupStats{}, err
	}
	if opts.Directory == "" {
		return BackupStats{}, ErrInvalidArgument
	}

	payload, err := json.Marshal(map[string]any{
		"directory":   opts.Directory,
		"include_wal": opts.IncludeWAL,
	})
	if err != nil {
		return BackupStats{}, err
	}

	requestURL := c.buildURL("/maintenance/backup", nil)
	var stats BackupStats

	err = c.doRequest(ctx, requestSpec{
		method: http.MethodPost,
		url:    requestURL,
		body:   payload,
		headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, reqOpts, func(resp *http.Response) error {
		return c.decodeBackup(resp, &stats)
	})
	if err != nil {
		return BackupStats{}, err
	}
	return stats, nil
}

type requestSpec struct {
	method  string
	url     string
	body    []byte
	headers map[string]string
}

func (c *HTTPClient) doRequest(
	ctx context.Context,
	spec requestSpec,
	opts RequestOptions,
	onSuccess func(*http.Response) error,
) error {
	if ctx == nil {
		return ErrInvalidArgument
	}

	disableRetry := opts.DisableRetry
	attempt := uint32(0)
	var lastErr error

	for {
		attempt++
		req, err := c.buildRequest(ctx, spec)
		if err != nil {
			return err
		}

		resp, err := c.httpClient.Do(req)
		if err == nil {
			err = c.handleResponse(resp, onSuccess)
		}

		if err == nil {
			return nil
		}
		lastErr = err
		if disableRetry {
			return err
		}
		if attempt >= c.retryPolicy.MaxAttempts {
			return errors.Join(ErrRetryExhausted, lastErr)
		}
		if !c.shouldRetry(err) {
			return err
		}

		delay := c.nextDelay(attempt)
		if err := sleepWithContext(ctx, delay); err != nil {
			return err
		}
	}
}

func (c *HTTPClient) buildRequest(
	ctx context.Context,
	spec requestSpec,
) (*http.Request, error) {
	var body io.Reader
	if spec.body != nil {
		body = bytes.NewReader(spec.body)
	}

	req, err := http.NewRequestWithContext(ctx, spec.method, spec.url, body)
	if err != nil {
		return nil, err
	}

	for key, value := range spec.headers {
		req.Header.Set(key, value)
	}
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	return req, nil
}

func (c *HTTPClient) handleResponse(
	resp *http.Response,
	onSuccess func(*http.Response) error,
) error {
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return c.readHTTPError(resp)
	}
	return onSuccess(resp)
}

func (c *HTTPClient) readHTTPError(resp *http.Response) error {
	body, err := c.readResponseBody(resp)
	if err != nil {
		return err
	}

	var payload map[string]string
	if err := json.Unmarshal(body, &payload); err == nil {
		if message, ok := payload["error"]; ok {
			return c.mapHTTPError(resp.StatusCode, message)
		}
	}

	return c.mapHTTPError(resp.StatusCode, string(body))
}

func (c *HTTPClient) readResponseBody(resp *http.Response) ([]byte, error) {
	reader := io.LimitReader(resp.Body, int64(c.maxResponseBytes))
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if uint64(len(body)) >= c.maxResponseBytes {
		return nil, ErrResponseTooLarge
	}
	return body, nil
}

func (c *HTTPClient) shouldRetry(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, ErrInvalidArgument) {
		return false
	}
	if errors.Is(err, ErrUnsupported) {
		return false
	}
	if errors.Is(err, ErrRequestFailed) {
		return false
	}
	if errors.Is(err, ErrResponseTooLarge) {
		return false
	}

	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		_, ok := c.retryStatus[httpErr.StatusCode]
		return ok
	}

	return true
}

func (c *HTTPClient) nextDelay(attempt uint32) time.Duration {
	delay := c.retryPolicy.BaseDelay * (1 << (attempt - 1))
	if delay > c.retryPolicy.MaxDelay {
		delay = c.retryPolicy.MaxDelay
	}
	if c.retryPolicy.Jitter > 0 {
		jitter := c.randomJitter(c.retryPolicy.Jitter)
		delay += jitter
		if delay < 0 {
			delay = 0
		}
	}
	return delay
}

func (c *HTTPClient) randomJitter(maxJitter time.Duration) time.Duration {
	c.rngMu.Lock()
	defer c.rngMu.Unlock()

	if maxJitter == 0 {
		return 0
	}

	jitter := time.Duration(c.rng.Int63n(int64(maxJitter)))
	return jitter - maxJitter/2
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func drainResponse(resp *http.Response) error {
	_, err := io.Copy(io.Discard, resp.Body)
	return err
}

func (c *HTTPClient) resolveEncodings(opts RequestOptions) (string, string) {
	keyEncoding := opts.KeyEncoding
	if keyEncoding == "" {
		keyEncoding = c.keyEncoding
	}

	valueEncoding := opts.ValueEncoding
	if valueEncoding == "" {
		valueEncoding = c.valueEncoding
	}
	return keyEncoding, valueEncoding
}

func (c *HTTPClient) buildURL(pathSuffix string, query url.Values) string {
	base := *c.baseURL
	base.Path = joinURLPath(base.Path, pathSuffix)
	if query != nil {
		base.RawQuery = query.Encode()
	}
	return base.String()
}

func joinURLPath(basePath string, suffix string) string {
	basePath = strings.TrimSuffix(basePath, "/")
	suffix = strings.TrimPrefix(suffix, "/")

	if basePath == "" {
		return "/" + suffix
	}
	if suffix == "" {
		return basePath
	}
	return basePath + "/" + suffix
}

func (c *HTTPClient) mapHTTPError(status int, message string) error {
	switch status {
	case http.StatusBadRequest:
		return ErrInvalidArgument
	case http.StatusNotFound:
		lower := strings.ToLower(message)
		if strings.Contains(lower, "account") {
			return ErrAccountNotFound
		}
		if strings.Contains(lower, "price plan") {
			return ErrPricePlanNotFound
		}
		if strings.Contains(lower, "vector") {
			return ErrVectorNotFound
		}
		return ErrKeyNotFound
	case http.StatusConflict:
		lower := strings.ToLower(message)
		switch {
		case strings.Contains(lower, "account already exists"):
			return ErrAccountExists
		case strings.Contains(lower, "insufficient credits"):
			return ErrInsufficientCredits
		case strings.Contains(lower, "duplicate event"):
			return ErrDuplicateEvent
		default:
			return ErrConflict
		}
	case http.StatusNotImplemented:
		return ErrUnsupported
	default:
		return &HTTPError{StatusCode: status, Message: message}
	}
}

func (c *HTTPClient) encodeKVEnvelope(
	key []byte,
	value []byte,
	keyEncoding string,
	valueEncoding string,
) ([]byte, error) {
	encodedKey, err := encodeBytes(key, keyEncoding)
	if err != nil {
		return nil, err
	}
	encodedValue, err := encodeBytes(value, valueEncoding)
	if err != nil {
		return nil, err
	}

	payload := map[string]string{
		"key":            encodedKey,
		"value":          encodedValue,
		"key_encoding":   keyEncoding,
		"value_encoding": valueEncoding,
	}
	return json.Marshal(payload)
}

func (c *HTTPClient) decodeKVResponse(resp *http.Response) ([]byte, error) {
	body, err := c.readResponseBody(resp)
	if err != nil {
		return nil, err
	}

	var kvResp struct {
		Value    string `json:"value"`
		Encoding string `json:"encoding"`
	}
	if err := json.Unmarshal(body, &kvResp); err != nil {
		return nil, err
	}
	if kvResp.Encoding == "" {
		return nil, ErrRequestFailed
	}
	return decodeBytes(kvResp.Value, kvResp.Encoding)
}

func (c *HTTPClient) encodeTxnRequest(
	ops []TransactionOperation,
	keyEncoding string,
	valueEncoding string,
) ([]byte, error) {
	type txnOperation struct {
		Type        string `json:"type"`
		Key         string `json:"key"`
		KeyEncoding string `json:"key_encoding,omitempty"`
		Value       string `json:"value,omitempty"`
		Encoding    string `json:"encoding,omitempty"`
	}

	encodedOps := make([]txnOperation, 0, len(ops))
	for _, op := range ops {
		encodedKey, err := encodeBytes(op.Key, keyEncoding)
		if err != nil {
			return nil, err
		}

		encodedOp := txnOperation{
			Type:        string(op.Type),
			Key:         encodedKey,
			KeyEncoding: keyEncoding,
		}

		if op.Type == OperationPut {
			encodedValue, err := encodeBytes(op.Value, valueEncoding)
			if err != nil {
				return nil, err
			}
			encodedOp.Value = encodedValue
			encodedOp.Encoding = valueEncoding
		}

		encodedOps = append(encodedOps, encodedOp)
	}

	payload := map[string]any{
		"operations": encodedOps,
	}
	return json.Marshal(payload)
}

func (c *HTTPClient) decodeTxnResponse(
	resp *http.Response,
	status *TransactionStatus,
) error {
	body, err := c.readResponseBody(resp)
	if err != nil {
		return err
	}

	var txnResp struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &txnResp); err != nil {
		return err
	}
	if txnResp.Status == "" {
		return ErrRequestFailed
	}
	*status = TransactionStatus(txnResp.Status)
	return nil
}

func (c *HTTPClient) encodeScanQuery(
	req ScanRequest,
	keyEncoding string,
	valueEncoding string,
) (url.Values, error) {
	query := url.Values{}
	query.Set("limit", formatUint(req.Limit))
	query.Set("key_encoding", keyEncoding)
	query.Set("value_encoding", valueEncoding)

	if len(req.Cursor) > 0 {
		encoded, err := encodeBytes(req.Cursor, keyEncoding)
		if err != nil {
			return nil, err
		}
		query.Set("cursor", encoded)
	}
	if len(req.Prefix) > 0 {
		encoded, err := encodeBytes(req.Prefix, keyEncoding)
		if err != nil {
			return nil, err
		}
		query.Set("prefix", encoded)
	}
	if req.IncludeValues {
		query.Set("include_values", "1")
		query.Set("max_value_bytes", formatUint(req.MaxValueBytes))
	}

	return query, nil
}

func (c *HTTPClient) decodeScanResponse(
	resp *http.Response,
	result *ScanResult,
	includeValues bool,
) error {
	body, err := c.readResponseBody(resp)
	if err != nil {
		return err
	}

	var scanResp struct {
		Entries       []scanEntryResponse `json:"entries"`
		NextCursor    string              `json:"next_cursor,omitempty"`
		KeyEncoding   string              `json:"key_encoding"`
		ValueEncoding string              `json:"value_encoding,omitempty"`
	}
	if err := json.Unmarshal(body, &scanResp); err != nil {
		return err
	}

	entries, err := decodeScanEntries(scanResp.Entries, scanResp.KeyEncoding, scanResp.ValueEncoding, includeValues)
	if err != nil {
		return err
	}

	nextCursor, err := decodeBytes(scanResp.NextCursor, scanResp.KeyEncoding)
	if err != nil {
		return err
	}

	result.Entries = entries
	result.NextCursor = nextCursor
	return nil
}

func (c *HTTPClient) decodeScanStream(
	resp *http.Response,
	handler func(ScanEntry) error,
	result *ScanResult,
	includeValues bool,
	keyEncoding string,
	valueEncoding string,
) error {
	limited := &io.LimitedReader{
		R: resp.Body,
		N: int64(c.maxResponseBytes),
	}
	decoder := json.NewDecoder(limited)

	for {
		var envelope scanStreamEnvelope
		if err := decoder.Decode(&envelope); err != nil {
			if errors.Is(err, io.EOF) {
				if limited.N == 0 {
					return ErrResponseTooLarge
				}
				return nil
			}
			return err
		}

		if envelope.Type == "cursor" {
			cursorEncoding := envelope.KeyEncoding
			if cursorEncoding == "" {
				cursorEncoding = keyEncoding
			}
			nextCursor, err := decodeBytes(
				envelope.NextCursor,
				cursorEncoding,
			)
			if err != nil {
				return err
			}
			result.NextCursor = nextCursor
			return nil
		}

		entry, err := decodeScanEntry(
			envelope,
			includeValues,
			keyEncoding,
			valueEncoding,
		)
		if err != nil {
			return err
		}
		if err := handler(entry); err != nil {
			return err
		}
	}
}

type scanStreamEnvelope struct {
	Type          string `json:"type,omitempty"`
	Key           string `json:"key,omitempty"`
	Value         string `json:"value,omitempty"`
	NextCursor    string `json:"next_cursor,omitempty"`
	KeyEncoding   string `json:"key_encoding,omitempty"`
	ValueEncoding string `json:"value_encoding,omitempty"`
}

type scanEntryResponse struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func decodeScanEntries(
	entries []scanEntryResponse,
	keyEncoding string,
	valueEncoding string,
	includeValues bool,
) ([]ScanEntry, error) {
	decoded := make([]ScanEntry, 0, len(entries))
	for _, entry := range entries {
		decodedEntry, err := decodeScanEntry(
			scanStreamEnvelope{
				Key:           entry.Key,
				Value:         entry.Value,
				KeyEncoding:   keyEncoding,
				ValueEncoding: valueEncoding,
			},
			includeValues,
			keyEncoding,
			valueEncoding,
		)
		if err != nil {
			return nil, err
		}
		decoded = append(decoded, decodedEntry)
	}
	return decoded, nil
}

func decodeScanEntry(
	envelope scanStreamEnvelope,
	includeValues bool,
	keyEncoding string,
	valueEncoding string,
) (ScanEntry, error) {
	encKey := envelope.KeyEncoding
	if encKey == "" {
		encKey = keyEncoding
	}
	encValue := envelope.ValueEncoding
	if encValue == "" {
		encValue = valueEncoding
	}

	key, err := decodeBytes(envelope.Key, encKey)
	if err != nil {
		return ScanEntry{}, err
	}

	if !includeValues {
		return ScanEntry{Key: key}, nil
	}

	value, err := decodeBytes(envelope.Value, encValue)
	if err != nil {
		return ScanEntry{}, err
	}

	return ScanEntry{
		Key:   key,
		Value: value,
	}, nil
}

func formatUint(value uint32) string {
	return strconv.FormatUint(uint64(value), 10)
}

func (c *HTTPClient) decodeVectorCreate(resp *http.Response, id *string) error {
	body, err := c.readResponseBody(resp)
	if err != nil {
		return err
	}

	var payload struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return err
	}
	if payload.ID == "" {
		return ErrRequestFailed
	}
	*id = payload.ID
	return nil
}

func (c *HTTPClient) decodeVector(resp *http.Response, vector *Vector) error {
	body, err := c.readResponseBody(resp)
	if err != nil {
		return err
	}

	var payload struct {
		ID       string                 `json:"id"`
		Values   []float64              `json:"values"`
		Metadata map[string]interface{} `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return err
	}
	if payload.ID == "" {
		return ErrRequestFailed
	}
	vector.ID = payload.ID
	vector.Values = payload.Values
	vector.Metadata = payload.Metadata
	return nil
}

func (c *HTTPClient) decodeCompact(resp *http.Response, stats *CompactStats) error {
	body, err := c.readResponseBody(resp)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(body, stats); err != nil {
		return err
	}
	return nil
}

func (c *HTTPClient) decodeSnapshot(resp *http.Response, stats *SnapshotStats) error {
	body, err := c.readResponseBody(resp)
	if err != nil {
		return err
	}

	var payload struct {
		Entries    uint32 `json:"entries"`
		Bytes      uint64 `json:"bytes"`
		Path       string `json:"path"`
		WALPath    string `json:"wal_path,omitempty"`
		WALBytes   uint64 `json:"wal_bytes,omitempty"`
		DurationMs int64  `json:"duration_ms"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return err
	}
	stats.Entries = payload.Entries
	stats.Bytes = payload.Bytes
	stats.Path = payload.Path
	stats.WALPath = payload.WALPath
	stats.WALBytes = payload.WALBytes
	stats.Duration = time.Duration(payload.DurationMs) * time.Millisecond
	return nil
}

func (c *HTTPClient) decodeBackup(resp *http.Response, stats *BackupStats) error {
	body, err := c.readResponseBody(resp)
	if err != nil {
		return err
	}

	var payload struct {
		Snapshot struct {
			Entries    uint32 `json:"entries"`
			Bytes      uint64 `json:"bytes"`
			Path       string `json:"path"`
			WALPath    string `json:"wal_path,omitempty"`
			WALBytes   uint64 `json:"wal_bytes,omitempty"`
			DurationMs int64  `json:"duration_ms"`
		} `json:"snapshot"`
		ManifestPath string `json:"manifest_path"`
		DurationMs   int64  `json:"duration_ms"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return err
	}
	stats.Snapshot = SnapshotStats{
		Entries:  payload.Snapshot.Entries,
		Bytes:    payload.Snapshot.Bytes,
		Path:     payload.Snapshot.Path,
		WALPath:  payload.Snapshot.WALPath,
		WALBytes: payload.Snapshot.WALBytes,
		Duration: time.Duration(payload.Snapshot.DurationMs) * time.Millisecond,
	}
	stats.ManifestPath = payload.ManifestPath
	stats.Duration = time.Duration(payload.DurationMs) * time.Millisecond
	return nil
}
