# Bulletant

Bulletant is a lightweight key/value store with pluggable storage backends,
transaction support, and optional durability via a write-ahead log (WAL).
It also includes an in-memory vector store for lightweight embedding use cases.

## Features
- Memory, partitioned memory, and disk storage backends
- Append-only disk format with in-memory index and compaction
- Transaction execution with commit/abort status
- WAL wrapper for durability and recovery
- Vector storage for memory-backed engines
- Typed errors for reliable `errors.Is` checks

## Quick start
```bash
go run cmd/server/main.go
go run cmd/server/main.go -backend=disk -data=bulletant.data -wal=bulletant.wal -compact
```

## Go usage
```go
store, _ := storage.NewDiskStorage("bulletant.data")
durable, _ := storage.OpenWALStorage(store, "bulletant.wal")
client := client.NewClient(durable)

_ = client.Put([]byte("hello"), []byte("world"))
value, _ := client.Get([]byte("hello"))
```

## Backends
- `MemoryStorage`: fast, in-memory store with vector support.
- `PartitionedStorage`: sharded by FNV-1a; transactions are executed per partition
  (not atomic across partitions).
- `DiskStorage`: append-only file with in-memory index. `Compact()` rewrites the
  file to the latest live keys only.
- `WALStorage`: wrapper that logs transactions before applying them to the
  underlying storage (supports `Compact()` if the wrapped storage does).

## Error handling
Use `errors.Is` with:
- `storage.ErrKeyNotFound`
- `storage.ErrVectorNotFound`
- `storage.ErrVectorUnsupported`
- `storage.ErrUnsupportedOperation`

## Notes
- Disk storage uses a new format with a header (`BANT`, version `1`). Legacy
  files without the header are still supported. Legacy deletes rely on the
  tombstone value `DELETED_DATA` (values equal to the sentinel are treated as
  deleted).

## Tests
```bash
go test ./...
```
