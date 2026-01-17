# Bulletant

Bulletant is a compact key-value store with transactions, optional persistence, a lightweight WAL, and a simple HTTP API. It supports multiple storage backends (memory, partitioned memory, disk) and an in-memory vector store for embeddings + metadata.

## Features
- Key-value API with `Put`, `Get`, `Delete`
- Transactions with multi-operation commit/abort semantics
- Optional write-ahead log (WAL) for recovery
- Disk storage with on-disk index rebuild
- HTTP server with JSON endpoints
- Vector store for embeddings + metadata

## Quick start
Run the HTTP server:

```
go run ./cmd/server --listen :8080 --storage memory
```

With disk storage + WAL:

```
go run ./cmd/server --listen :8080 --storage disk --data ./bulletant.db --wal ./bulletant.wal
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

Vector store:

```
curl -X POST "http://localhost:8080/vectors" \
  -H "Content-Type: application/json" \
  -d '{"values":[0.1,0.2],"metadata":{"tag":"demo"}}'

curl "http://localhost:8080/vectors/<id>"
```

## Packages
- `internal/storage`: storage backends and vector store
- `internal/transaction`: transaction model
- `internal/log`: WAL implementation
- `internal/db`: DB orchestration + WAL integration
- `internal/server`: HTTP handlers

## Testing
```
go test ./...
```
