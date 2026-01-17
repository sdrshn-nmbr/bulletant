package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
	"golang.org/x/exp/mmap"
)

type diskFormat uint8

const (
	diskFormatV0 diskFormat = 0
	diskFormatV1 diskFormat = 1
)

const (
	diskMagic      = "BANT"
	diskHeaderSize = 5
	TOMBSTONE      = "DELETED_DATA"
)

type diskIndexEntry struct {
	offset   int64
	valueLen uint32
	deleted  bool
}

type DiskStorage struct {
	file       *os.File
	mmap       *mmap.ReaderAt
	size       int64
	mu         sync.RWMutex
	index      map[string]diskIndexEntry
	format     diskFormat
	dataOffset int64
}

func NewDiskStorage(filename string) (*DiskStorage, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	d := &DiskStorage{
		file:  file,
		size:  info.Size(),
		index: make(map[string]diskIndexEntry),
	}

	if err := d.initFormat(); err != nil {
		file.Close()
		return nil, err
	}

	if err := d.remapFile(); err != nil {
		file.Close()
		return nil, err
	}

	if err := d.buildIndex(); err != nil {
		d.Close()
		return nil, err
	}

	return d, nil
}

func (d *DiskStorage) ExecuteTransaction(t *transaction.Transaction) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, op := range t.Operations {
		switch op.Type {
		case types.Put:
			if d.format == diskFormatV0 && bytes.Equal(op.Value, []byte(TOMBSTONE)) {
				t.Status = transaction.Aborted
				return ErrReservedValue
			}
		case types.Delete:
			entry, ok := d.index[string(op.Key)]
			if !ok || entry.deleted {
				t.Status = transaction.Aborted
				return ErrKeyNotFound
			}
		default:
			t.Status = transaction.Aborted
			return ErrUnsupported
		}
	}

	for _, op := range t.Operations {
		switch op.Type {
		case types.Put:
			if err := d.putLocked(op.Key, op.Value); err != nil {
				t.Status = transaction.Aborted
				return err
			}
		case types.Delete:
			if err := d.deleteLocked(op.Key); err != nil {
				t.Status = transaction.Aborted
				return err
			}
		default:
			t.Status = transaction.Aborted
			return ErrUnsupported
		}
	}

	t.Status = transaction.Committed
	return nil
}

func (d *DiskStorage) Put(key types.Key, value types.Value) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.putLocked(key, value)
}

func (d *DiskStorage) Get(key types.Key) (types.Value, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entry, ok := d.index[string(key)]
	if !ok || entry.deleted {
		return nil, ErrKeyNotFound
	}

	value := make([]byte, entry.valueLen)
	if entry.valueLen == 0 {
		return value, nil
	}

	if _, err := d.readAt(value, entry.offset); err != nil {
		return nil, err
	}

	return value, nil
}

func (d *DiskStorage) Delete(key types.Key) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.deleteLocked(key)
}

func (d *DiskStorage) Scan(req ScanRequest) (ScanResult, error) {
	if err := req.Validate(); err != nil {
		return ScanResult{}, err
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	keys := d.scanKeys(req.Prefix)
	sort.Strings(keys)

	start := scanStartIndex(keys, string(req.Cursor))
	if start >= len(keys) {
		return ScanResult{Entries: []ScanEntry{}}, nil
	}

	limit := int(req.Limit)
	end := start + limit
	if end > len(keys) {
		end = len(keys)
	}

	entries := make([]ScanEntry, 0, end-start)
	for _, key := range keys[start:end] {
		entry := d.index[key]
		scanEntry := ScanEntry{Key: types.Key(key)}
		if req.IncludeValues {
			value, err := d.readValue(entry, req)
			if err != nil {
				return ScanResult{}, err
			}
			scanEntry.Value = value
		}
		entries = append(entries, scanEntry)
	}

	nextCursor := types.Key("")
	if end < len(keys) {
		nextCursor = types.Key(keys[end-1])
	}

	return ScanResult{
		Entries:    entries,
		NextCursor: nextCursor,
	}, nil
}

func (d *DiskStorage) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.mmap != nil {
		if err := d.mmap.Close(); err != nil {
			return err
		}
	}
	return d.file.Close()
}

func (d *DiskStorage) AddVector(values []float64, metadata map[string]interface{}) (string, error) {
	return "", ErrUnsupported
}

func (d *DiskStorage) GetVector(id string) (*Vector, error) {
	return nil, ErrUnsupported
}

func (d *DiskStorage) DeleteVector(id string) error {
	return ErrUnsupported
}

func (d *DiskStorage) Compact(opts CompactOptions) (CompactStats, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	stats := CompactStats{
		BytesBefore: uint64(d.size),
	}

	if err := d.validateCompactOptions(&opts); err != nil {
		return stats, err
	}

	keys := d.scanKeys(types.Key(""))
	sort.Strings(keys)

	if len(keys) > int(^uint32(0)) {
		return stats, ErrInvalidCompactLimit
	}
	stats.EntriesTotal = uint32(len(keys))
	if stats.EntriesTotal > opts.MaxEntries {
		return stats, ErrCompactLimitExceeded
	}

	estimatedBytes, err := d.compactEstimatedBytes(keys)
	if err != nil {
		return stats, err
	}
	if estimatedBytes > opts.MaxBytes {
		return stats, ErrCompactLimitExceeded
	}

	tempPath := opts.TempPath
	if tempPath == "" {
		tempPath = d.defaultCompactPath()
	}
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = os.Remove(tempPath)
		}
	}()

	newIndex := make(map[string]diskIndexEntry, len(keys))
	entriesWritten, bytesWritten, err := d.writeCompactFile(
		tempPath,
		keys,
		newIndex,
	)
	if err != nil {
		return stats, err
	}

	stats.EntriesWritten = entriesWritten
	stats.BytesAfter = bytesWritten

	if err := d.swapCompactFile(tempPath, newIndex); err != nil {
		return stats, err
	}
	cleanupTemp = false

	return stats, nil
}

func (d *DiskStorage) putLocked(key types.Key, value types.Value) error {
	if d.format == diskFormatV0 && bytes.Equal(value, []byte(TOMBSTONE)) {
		return ErrReservedValue
	}

	entry, err := d.appendEntryLocked(types.Put, key, value)
	if err != nil {
		return err
	}

	d.index[string(key)] = entry
	return nil
}

func (d *DiskStorage) deleteLocked(key types.Key) error {
	existing, ok := d.index[string(key)]
	if !ok || existing.deleted {
		return ErrKeyNotFound
	}

	var value []byte
	if d.format == diskFormatV0 {
		value = []byte(TOMBSTONE)
	}

	entry, err := d.appendEntryLocked(types.Delete, key, value)
	if err != nil {
		return err
	}

	entry.deleted = true
	d.index[string(key)] = entry
	return nil
}

func (d *DiskStorage) appendEntryLocked(op types.OperationType, key types.Key, value []byte) (diskIndexEntry, error) {
	if d.format == diskFormatV1 && op == types.Delete {
		value = nil
	}

	keyBytes := []byte(key)
	entrySize := d.entrySize(op, keyBytes, value)
	startOffset := d.size

	offset := startOffset
	if d.format == diskFormatV1 {
		if _, err := d.file.WriteAt([]byte{byte(op)}, offset); err != nil {
			return diskIndexEntry{}, err
		}
		offset++
	}

	if _, err := d.file.WriteAt(uint32ToBytes(uint32(len(keyBytes))), offset); err != nil {
		return diskIndexEntry{}, err
	}
	offset += 4

	if _, err := d.file.WriteAt(keyBytes, offset); err != nil {
		return diskIndexEntry{}, err
	}
	offset += int64(len(keyBytes))

	if _, err := d.file.WriteAt(uint32ToBytes(uint32(len(value))), offset); err != nil {
		return diskIndexEntry{}, err
	}
	offset += 4

	valueOffset := offset
	if len(value) > 0 {
		if _, err := d.file.WriteAt(value, offset); err != nil {
			return diskIndexEntry{}, err
		}
	}

	d.size = startOffset + int64(entrySize)
	if err := d.remapFile(); err != nil {
		return diskIndexEntry{}, err
	}

	return diskIndexEntry{
		offset:   valueOffset,
		valueLen: uint32(len(value)),
		deleted:  op == types.Delete,
	}, nil
}

func (d *DiskStorage) entrySize(op types.OperationType, key []byte, value []byte) int {
	if d.format == diskFormatV1 {
		return 1 + 4 + len(key) + 4 + len(value)
	}
	return 4 + len(key) + 4 + len(value)
}

func (d *DiskStorage) entrySizeForFormat(
	format diskFormat,
	key []byte,
	value []byte,
) int {
	if format == diskFormatV1 {
		return 1 + 4 + len(key) + 4 + len(value)
	}
	return 4 + len(key) + 4 + len(value)
}

func (d *DiskStorage) entrySizeForFormatLen(
	format diskFormat,
	keyLen int,
	valueLen int,
) int {
	if format == diskFormatV1 {
		return 1 + 4 + keyLen + 4 + valueLen
	}
	return 4 + keyLen + 4 + valueLen
}

func (d *DiskStorage) initFormat() error {
	if d.size == 0 {
		header := append([]byte(diskMagic), byte(diskFormatV1))
		if _, err := d.file.WriteAt(header, 0); err != nil {
			return err
		}
		d.size = int64(len(header))
		d.format = diskFormatV1
		d.dataOffset = diskHeaderSize
		return nil
	}

	if d.size < diskHeaderSize {
		return ErrCorruptData
	}

	header := make([]byte, diskHeaderSize)
	if _, err := d.file.ReadAt(header, 0); err != nil {
		return err
	}

	if string(header[:4]) == diskMagic && header[4] == byte(diskFormatV1) {
		d.format = diskFormatV1
		d.dataOffset = diskHeaderSize
		return nil
	}

	d.format = diskFormatV0
	d.dataOffset = 0
	return nil
}

func (d *DiskStorage) buildIndex() error {
	if d.size == 0 {
		return nil
	}

	offset := d.dataOffset
	for offset < d.size {
		var op types.OperationType
		if d.format == diskFormatV1 {
			value, err := d.readUint8(offset)
			if err != nil {
				return err
			}
			op = types.OperationType(value)
			if op != types.Put && op != types.Delete {
				return ErrCorruptData
			}
			offset++
		} else {
			op = types.Put
		}

		keyLen, err := d.readUint32(offset)
		if err != nil {
			return err
		}
		offset += 4

		if offset+int64(keyLen) > d.size {
			return ErrCorruptData
		}

		key := make([]byte, keyLen)
		if _, err := d.readAt(key, offset); err != nil {
			return err
		}
		offset += int64(keyLen)

		valLen, err := d.readUint32(offset)
		if err != nil {
			return err
		}
		offset += 4

		if offset+int64(valLen) > d.size {
			return ErrCorruptData
		}

		deleted := op == types.Delete
		if d.format == diskFormatV0 && !deleted {
			isTombstone, err := d.isTombstoneAt(offset, valLen)
			if err != nil {
				return err
			}
			deleted = isTombstone
		}

		d.index[string(key)] = diskIndexEntry{
			offset:   offset,
			valueLen: valLen,
			deleted:  deleted,
		}

		offset += int64(valLen)
	}

	return nil
}

func (d *DiskStorage) isTombstoneAt(offset int64, valueLen uint32) (bool, error) {
	tomb := []byte(TOMBSTONE)
	if int(valueLen) != len(tomb) {
		return false, nil
	}

	buf := make([]byte, len(tomb))
	if _, err := d.readAt(buf, offset); err != nil {
		return false, err
	}

	return bytes.Equal(buf, tomb), nil
}

func (d *DiskStorage) remapFile() error {
	if d.mmap != nil {
		if err := d.mmap.Close(); err != nil {
			return err
		}
		d.mmap = nil
	}

	info, err := d.file.Stat()
	if err != nil {
		return err
	}
	d.size = info.Size()

	if d.size == 0 {
		return nil
	}

	mmapFile, err := mmap.Open(d.file.Name())
	if err != nil {
		return err
	}
	d.mmap = mmapFile
	return nil
}

func (d *DiskStorage) scanKeys(prefix types.Key) []string {
	keys := make([]string, 0, len(d.index))
	for key, entry := range d.index {
		if entry.deleted {
			continue
		}
		keys = append(keys, key)
	}
	return filterKeys(keys, string(prefix))
}

func (d *DiskStorage) readValue(
	entry diskIndexEntry,
	req ScanRequest,
) (types.Value, error) {
	maxInt := int(^uint(0) >> 1)
	if entry.valueLen > uint32(maxInt) {
		return nil, ErrValueTooLarge
	}
	if entry.valueLen > req.MaxValueBytes {
		return nil, ErrValueTooLarge
	}
	if entry.valueLen == 0 {
		return []byte{}, nil
	}

	value := make([]byte, entry.valueLen)
	if _, err := d.readAt(value, entry.offset); err != nil {
		return nil, err
	}
	return value, nil
}

func (d *DiskStorage) validateCompactOptions(opts *CompactOptions) error {
	if opts.MaxEntries == 0 {
		return ErrInvalidCompactLimit
	}
	if opts.MaxBytes == 0 {
		return ErrInvalidCompactLimit
	}
	return nil
}

func (d *DiskStorage) compactEstimatedBytes(keys []string) (uint64, error) {
	total := uint64(diskHeaderSize)
	for _, key := range keys {
		entry := d.index[key]
		keyBytes := []byte(key)
		maxInt := int(^uint(0) >> 1)
		if entry.valueLen > uint32(maxInt) {
			return 0, ErrValueTooLarge
		}
		valueLen := int(entry.valueLen)
		size := d.entrySizeForFormatLen(
			diskFormatV1,
			len(keyBytes),
			valueLen,
		)
		total += uint64(size)
	}
	return total, nil
}

func (d *DiskStorage) writeCompactFile(
	path string,
	keys []string,
	newIndex map[string]diskIndexEntry,
) (uint32, uint64, error) {
	tempFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return 0, 0, err
	}
	defer tempFile.Close()

	if err := d.writeCompactHeader(tempFile); err != nil {
		return 0, 0, err
	}

	offset := int64(diskHeaderSize)
	var entriesWritten uint32
	for _, key := range keys {
		entry := d.index[key]
		value, err := d.readValue(entry, ScanRequest{
			Limit:         1,
			IncludeValues: true,
			MaxValueBytes: entry.valueLen,
		})
		if err != nil {
			return 0, 0, err
		}

		keyBytes := []byte(key)
		entrySize := d.entrySizeForFormat(diskFormatV1, keyBytes, value)

		written, valueOffset, err := d.writeEntryTo(
			tempFile,
			offset,
			keyBytes,
			value,
		)
		if err != nil {
			return 0, 0, err
		}
		offset += int64(written)
		entriesWritten++

		newIndex[key] = diskIndexEntry{
			offset:   valueOffset,
			valueLen: uint32(len(value)),
			deleted:  false,
		}

		if written != entrySize {
			return 0, 0, io.ErrShortWrite
		}
	}

	if err := tempFile.Sync(); err != nil {
		return 0, 0, err
	}

	return entriesWritten, uint64(offset), nil
}

func (d *DiskStorage) writeCompactHeader(file *os.File) error {
	header := append([]byte(diskMagic), byte(diskFormatV1))
	_, err := file.WriteAt(header, 0)
	return err
}

func (d *DiskStorage) writeEntryTo(
	file *os.File,
	offset int64,
	key []byte,
	value []byte,
) (int, int64, error) {
	start := offset
	if _, err := file.WriteAt([]byte{byte(types.Put)}, offset); err != nil {
		return 0, 0, err
	}
	offset++

	if _, err := file.WriteAt(uint32ToBytes(uint32(len(key))), offset); err != nil {
		return 0, 0, err
	}
	offset += 4

	if _, err := file.WriteAt(key, offset); err != nil {
		return 0, 0, err
	}
	offset += int64(len(key))

	if _, err := file.WriteAt(uint32ToBytes(uint32(len(value))), offset); err != nil {
		return 0, 0, err
	}
	offset += 4

	valueOffset := offset
	if len(value) > 0 {
		if _, err := file.WriteAt(value, offset); err != nil {
			return 0, 0, err
		}
		offset += int64(len(value))
	}

	return int(offset - start), valueOffset, nil
}

func (d *DiskStorage) swapCompactFile(
	tempPath string,
	newIndex map[string]diskIndexEntry,
) error {
	originalPath := d.file.Name()
	backupPath := filepath.Join(
		filepath.Dir(originalPath),
		filepath.Base(originalPath)+".bak",
	)

	if err := d.closeLocked(); err != nil {
		return err
	}

	_ = os.Remove(backupPath)
	if err := os.Rename(originalPath, backupPath); err != nil {
		return err
	}
	if err := os.Rename(tempPath, originalPath); err != nil {
		_ = os.Rename(backupPath, originalPath)
		return err
	}
	_ = os.Remove(backupPath)

	file, err := os.OpenFile(originalPath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	d.file = file
	d.index = newIndex
	d.format = diskFormatV1
	d.dataOffset = diskHeaderSize

	return d.remapFile()
}

func (d *DiskStorage) closeLocked() error {
	if d.mmap != nil {
		if err := d.mmap.Close(); err != nil {
			return err
		}
		d.mmap = nil
	}
	return d.file.Close()
}

func (d *DiskStorage) defaultCompactPath() string {
	path := d.file.Name()
	return path + ".compact"
}

func (d *DiskStorage) readAt(p []byte, off int64) (int, error) {
	if d.mmap != nil {
		return d.mmap.ReadAt(p, off)
	}
	return d.file.ReadAt(p, off)
}

func (d *DiskStorage) readUint32(offset int64) (uint32, error) {
	buf := make([]byte, 4)
	if _, err := d.readAt(buf, offset); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func (d *DiskStorage) readUint8(offset int64) (uint8, error) {
	buf := make([]byte, 1)
	if _, err := d.readAt(buf, offset); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func uint32ToBytes(u uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, u)
	return b
}

