# Bulletant

Bulletant is a compact key-value store with transactions, optional persistence, a lightweight WAL, and a simple HTTP API. It supports multiple storage backends (memory, partitioned memory, disk) and an in-memory vector store for embeddings + metadata.

## Features
- Key-value API with `Put`, `Get`, `Delete`
- Transactions with multi-operation commit/abort semantics
- Optional write-ahead log (WAL) for recovery
- Disk storage with on-disk index rebuild and compaction
- LSM storage with memtables, segments, and bloom filters
- HTTP server with JSON endpoints
- Streaming scan API with cursor + prefix
- Local + HTTP client SDK with retries/backoff
- Vector store for embeddings + metadata
- Background compaction scheduler with rate limiting
- Snapshots + backups
- CLI with local and HTTP modes

## Quick start
Run the HTTP server:

```
go run ./cmd/server --listen :8080 --storage memory
```

With disk storage + WAL:

```
go run ./cmd/server --listen :8080 --storage disk --data ./bulletant.db --wal ./bulletant.wal
```

With LSM storage:

```
go run ./cmd/server --listen :8080 --storage lsm --lsm-dir ./bulletant.lsm
```

Enable background compaction:

```
go run ./cmd/server --listen :8080 --storage lsm --lsm-dir ./bulletant.lsm \
  --compaction-interval 30s --compaction-max-entries 100000 --compaction-max-bytes 104857600 \
  --compaction-rate-limit-bytes 4194304
```

## HTTP API
Key-value operations:

```
curl -X PUT "http://localhost:8080/kv/name" \
  -H "Content-Type: application/json" \
  -d '{"value":"Ada"}'

curl "http://localhost:8080/kv/name"
curl "http://localhost:8080/kv/name?raw=1"

curl -X DELETE "http://localhost:8080/kv/name"
```

Key-value with encoded keys (base64):
```
curl -X POST "http://localhost:8080/kv" \
  -H "Content-Type: application/json" \
  -d '{"key":"bmFtZQ==","value":"QWRh","key_encoding":"base64","value_encoding":"base64"}'

curl "http://localhost:8080/kv?key=bmFtZQ==&key_encoding=base64&value_encoding=base64"
```

Transactions:

```
curl -X POST "http://localhost:8080/txn" \
  -H "Content-Type: application/json" \
  -d '{
    "operations":[
      {"type":"put","key":"user","value":"YWxpY2U=","encoding":"base64"},
      {"type":"delete","key":"old"}
    ]
  }'
```

Scan:
```
curl "http://localhost:8080/scan?limit=2&include_values=1&max_value_bytes=1024&key_encoding=base64&value_encoding=base64"

curl "http://localhost:8080/scan?limit=100&stream=1&key_encoding=base64&value_encoding=base64"
```

Compaction (disk storage only):
```
curl -X POST "http://localhost:8080/maintenance/compact" \
  -H "Content-Type: application/json" \
  -d '{"max_entries":100000,"max_bytes":10485760}'
```

Snapshots + backups:
```
curl -X POST "http://localhost:8080/maintenance/snapshot" \
  -H "Content-Type: application/json" \
  -d '{"path":"./snapshots/bulletant.snapshot"}'

curl -X POST "http://localhost:8080/maintenance/backup" \
  -H "Content-Type: application/json" \
  -d '{"directory":"./backups"}'
```

Vector store:

```
curl -X POST "http://localhost:8080/vectors" \
  -H "Content-Type: application/json" \
  -d '{"values":[0.1,0.2],"metadata":{"tag":"demo"}}'

curl "http://localhost:8080/vectors/<id>"
```

## Client SDK
Local client:
```
local, err := client.OpenLocal(client.LocalOptions{
  StorageType: client.StorageDisk,
  DataPath:    "./bulletant.db",
  WALPath:     "./bulletant.wal",
})
```

Local client with LSM:
```
local, err := client.OpenLocal(client.LocalOptions{
  StorageType:          client.StorageLSM,
  LSMDir:               "./bulletant.lsm",
  LSMMemtableMaxEntries: 50000,
  LSMMemtableMaxBytes:   8 << 20,
  LSMSegmentMaxBytes:    16 << 20,
})
```

HTTP client:
```
httpClient, err := client.NewHTTPClient(client.HTTPOptions{
  BaseURL:          "http://localhost:8080",
  HTTPClient:       &http.Client{Timeout: 2 * time.Second},
  RetryPolicy:      client.DefaultRetryPolicy(),
  KeyEncoding:      "base64",
  ValueEncoding:    "base64",
  MaxResponseBytes: 1 << 20,
})
```

Snapshot + backup via client:
```
stats, err := local.Snapshot(context.Background(), client.SnapshotOptions{
  Path: "./snapshots/bulletant.snapshot",
})

backup, err := httpClient.Backup(context.Background(), client.BackupOptions{
  Directory: "./backups",
})
```

## CLI
Local mode:
```
go run ./cmd/bulletant --mode=local --storage-type=memory put key value
go run ./cmd/bulletant --mode=local --storage-type=lsm --lsm-dir ./bulletant.lsm get key
```

HTTP mode:
```
go run ./cmd/bulletant --mode=http --base-url http://localhost:8080 put key value
go run ./cmd/bulletant --mode=http scan --limit 10 --include-values
```

## Packages
- `internal/storage`: storage backends and vector store
- `internal/transaction`: transaction model
- `internal/log`: WAL implementation
- `internal/db`: DB orchestration + WAL integration
- `internal/server`: HTTP handlers
- `internal/maintenance`: compaction scheduler + rate limiter
- `pkg/client`: local + HTTP SDK

## Testing
```
go test ./...
```
