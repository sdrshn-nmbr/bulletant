package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
	"golang.org/x/exp/mmap"
)

const (
	diskMagic      = "BANT"
	diskVersion    = uint8(1)
	diskHeaderSize = 5
)

const legacyTombstone string = "DELETED_DATA"

type recordType uint8

const (
	recordPut recordType = iota + 1
	recordDelete
)

type diskIndexEntry struct {
	offset   int64
	valueLen uint32
	deleted  bool
}

type DiskStorage struct {
	file   *os.File
	mmap   *mmap.ReaderAt
	size   int64
	mu     sync.RWMutex
	index  map[string]diskIndexEntry
	legacy bool
	path   string
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
		path:  filename,
	}

	if d.size == 0 {
		if err := d.writeHeader(); err != nil {
			file.Close()
			return nil, err
		}
	}

	if err := d.detectFormat(); err != nil {
		file.Close()
		return nil, err
	}

	if err := d.loadIndex(); err != nil {
		file.Close()
		return nil, err
	}

	if err := d.remapFile(); err != nil {
		file.Close()
		return nil, err
	}

	return d, nil
}

func (d *DiskStorage) ExecuteTransaction(t *transaction.Transaction) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.validateTransactionLocked(t); err != nil {
		t.Status = transaction.Aborted
		return err
	}

	startSize := d.size
	previous := d.snapshotIndexLocked(t)

	var err error
	if d.legacy {
		err = d.applyLegacyTransactionLocked(t)
	} else {
		err = d.applyV1TransactionLocked(t)
	}

	if err != nil {
		_ = d.file.Truncate(startSize)
		_ = d.remapFile()
		d.restoreIndexLocked(previous)
		t.Status = transaction.Aborted
		return err
	}

	t.Status = transaction.Committed
	return nil
}

func (d *DiskStorage) Put(key types.Key, value types.Value) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.appendPutLocked(key, value)
	return err
}

func (d *DiskStorage) Get(key types.Key) (types.Value, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entry, ok := d.index[string(key)]
	if !ok || entry.deleted {
		return nil, ErrKeyNotFound
	}

	value := make([]byte, entry.valueLen)
	_, err := d.mmap.ReadAt(value, entry.offset)
	if err != nil {
		return nil, err
	}

	if d.legacy && string(value) == legacyTombstone {
		return nil, ErrKeyNotFound
	}

	return value, nil
}

func (d *DiskStorage) Delete(key types.Key) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	entry, ok := d.index[string(key)]
	if !ok || entry.deleted {
		return ErrKeyNotFound
	}

	_, err := d.appendDeleteLocked(key)
	return err
}

func (d *DiskStorage) Compact() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	tmpPath := d.path + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	cleanup := func() {
		tmpFile.Close()
		_ = os.Remove(tmpPath)
	}

	legacy := d.legacy
	var offset int64
	if !legacy {
		header := make([]byte, diskHeaderSize)
		copy(header, []byte(diskMagic))
		header[4] = diskVersion
		if _, err := tmpFile.WriteAt(header, 0); err != nil {
			cleanup()
			return err
		}
		offset = diskHeaderSize
	}

	newIndex := make(map[string]diskIndexEntry, len(d.index))
	for key, entry := range d.index {
		if entry.deleted {
			continue
		}
		value := make([]byte, entry.valueLen)
		if _, err := d.mmap.ReadAt(value, entry.offset); err != nil {
			cleanup()
			return err
		}
		valueOffset, valueLen, err := writeRecordAt(tmpFile, legacy, offset, recordPut, types.Key(key), value)
		if err != nil {
			cleanup()
			return err
		}
		newIndex[key] = diskIndexEntry{
			offset:   valueOffset,
			valueLen: valueLen,
		}
		offset += recordSize(legacy, types.Key(key), value)
	}

	if err := tmpFile.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		cleanup()
		return err
	}

	if d.mmap != nil {
		if err := d.mmap.Close(); err != nil {
			_ = os.Remove(tmpPath)
			return err
		}
	}
	if err := d.file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, d.path); err != nil {
		return err
	}

	file, err := os.OpenFile(d.path, os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	d.file = file
	d.size = info.Size()
	d.index = newIndex
	d.legacy = legacy

	if err := d.remapFile(); err != nil {
		file.Close()
		return err
	}

	return nil
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
	return "", ErrVectorUnsupported
}

func (d *DiskStorage) GetVector(id string) (*Vector, error) {
	return nil, ErrVectorUnsupported
}

func (d *DiskStorage) DeleteVector(id string) error {
	return ErrVectorUnsupported
}

func (d *DiskStorage) writeHeader() error {
	header := make([]byte, diskHeaderSize)
	copy(header, []byte(diskMagic))
	header[4] = diskVersion
	if _, err := d.file.WriteAt(header, 0); err != nil {
		return err
	}
	if err := d.file.Sync(); err != nil {
		return err
	}
	d.size = diskHeaderSize
	return nil
}

func (d *DiskStorage) detectFormat() error {
	if d.size >= diskHeaderSize {
		var header [diskHeaderSize]byte
		if _, err := d.file.ReadAt(header[:], 0); err != nil {
			return err
		}
		if string(header[:4]) == diskMagic {
			if header[4] != diskVersion {
				return fmt.Errorf("%w: %d", ErrUnsupportedVersion, header[4])
			}
			d.legacy = false
			return nil
		}
	}

	if d.size == 0 {
		d.legacy = false
		return nil
	}

	d.legacy = true
	return nil
}

func (d *DiskStorage) loadIndex() error {
	if d.legacy {
		return d.loadLegacyIndex()
	}
	return d.loadV1Index()
}

func (d *DiskStorage) loadV1Index() error {
	offset := int64(diskHeaderSize)
	for offset < d.size {
		recType, err := d.readUint8(offset)
		if err != nil {
			return err
		}
		offset++

		keyLen, err := d.readUint32(offset)
		if err != nil {
			return err
		}
		offset += 4

		if offset+int64(keyLen)+4 > d.size {
			return fmt.Errorf("%w: invalid key length", ErrCorruptData)
		}

		key := make([]byte, keyLen)
		if _, err := d.file.ReadAt(key, offset); err != nil {
			return err
		}
		offset += int64(keyLen)

		valueLen, err := d.readUint32(offset)
		if err != nil {
			return err
		}
		offset += 4

		valueOffset := offset
		if offset+int64(valueLen) > d.size {
			return fmt.Errorf("%w: invalid value length", ErrCorruptData)
		}
		offset += int64(valueLen)

		switch recordType(recType) {
		case recordPut:
			d.index[string(key)] = diskIndexEntry{
				offset:   valueOffset,
				valueLen: valueLen,
			}
		case recordDelete:
			d.index[string(key)] = diskIndexEntry{deleted: true}
		default:
			return fmt.Errorf("%w: invalid record type", ErrCorruptData)
		}
	}

	return nil
}

func (d *DiskStorage) loadLegacyIndex() error {
	offset := int64(0)
	for offset < d.size {
		keyLen, err := d.readUint32(offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("%w: unexpected end of file", ErrCorruptData)
			}
			return err
		}
		offset += 4

		if offset+int64(keyLen)+4 > d.size {
			return fmt.Errorf("%w: invalid key length", ErrCorruptData)
		}

		key := make([]byte, keyLen)
		if _, err := d.file.ReadAt(key, offset); err != nil {
			return err
		}
		offset += int64(keyLen)

		valueLen, err := d.readUint32(offset)
		if err != nil {
			return err
		}
		offset += 4

		valueOffset := offset
		if offset+int64(valueLen) > d.size {
			return fmt.Errorf("%w: invalid value length", ErrCorruptData)
		}

		deleted := false
		if valueLen == uint32(len(legacyTombstone)) {
			value := make([]byte, valueLen)
			if _, err := d.file.ReadAt(value, offset); err != nil {
				return err
			}
			if string(value) == legacyTombstone {
				deleted = true
			}
		}

		d.index[string(key)] = diskIndexEntry{
			offset:   valueOffset,
			valueLen: valueLen,
			deleted:  deleted,
		}

		offset += int64(valueLen)
	}

	return nil
}

func (d *DiskStorage) remapFile() error {
	if d.mmap != nil {
		if err := d.mmap.Close(); err != nil {
			return err
		}
	}

	mmapFile, err := mmap.Open(d.file.Name())
	if err != nil {
		return err
	}
	d.mmap = mmapFile

	info, err := d.file.Stat()
	if err != nil {
		return err
	}
	d.size = info.Size()
	return nil
}

func (d *DiskStorage) readUint32(offset int64) (uint32, error) {
	var buf [4]byte
	if _, err := d.file.ReadAt(buf[:], offset); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func (d *DiskStorage) readUint8(offset int64) (uint8, error) {
	var buf [1]byte
	if _, err := d.file.ReadAt(buf[:], offset); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func recordSize(legacy bool, key types.Key, value types.Value) int64 {
	if legacy {
		return int64(4 + len(key) + 4 + len(value))
	}
	return int64(1 + 4 + len(key) + 4 + len(value))
}

func writeRecordAt(file *os.File, legacy bool, offset int64, recType recordType, key types.Key, value types.Value) (int64, uint32, error) {
	if legacy {
		keyLen := uint32(len(key))
		valueLen := uint32(len(value))

		if err := writeUint32At(file, offset, keyLen); err != nil {
			return 0, 0, err
		}
		offset += 4
		if _, err := file.WriteAt([]byte(key), offset); err != nil {
			return 0, 0, err
		}
		offset += int64(keyLen)
		if err := writeUint32At(file, offset, valueLen); err != nil {
			return 0, 0, err
		}
		offset += 4
		if _, err := file.WriteAt(value, offset); err != nil {
			return 0, 0, err
		}
		return offset, valueLen, nil
	}

	keyLen := uint32(len(key))
	valueLen := uint32(len(value))
	if recType == recordDelete {
		valueLen = 0
	}

	if _, err := file.WriteAt([]byte{byte(recType)}, offset); err != nil {
		return 0, 0, err
	}
	offset++
	if err := writeUint32At(file, offset, keyLen); err != nil {
		return 0, 0, err
	}
	offset += 4
	if _, err := file.WriteAt([]byte(key), offset); err != nil {
		return 0, 0, err
	}
	offset += int64(keyLen)
	if err := writeUint32At(file, offset, valueLen); err != nil {
		return 0, 0, err
	}
	offset += 4
	if valueLen > 0 {
		if _, err := file.WriteAt(value, offset); err != nil {
			return 0, 0, err
		}
	}
	return offset, valueLen, nil
}

func writeUint32At(file *os.File, offset int64, v uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	_, err := file.WriteAt(buf[:], offset)
	return err
}

func (d *DiskStorage) appendPutLocked(key types.Key, value types.Value) (diskIndexEntry, error) {
	if d.legacy {
		return d.appendLegacyRecordLocked(recordPut, key, value)
	}
	return d.appendV1RecordLocked(recordPut, key, value)
}

func (d *DiskStorage) appendDeleteLocked(key types.Key) (diskIndexEntry, error) {
	if d.legacy {
		return d.appendLegacyRecordLocked(recordDelete, key, []byte(legacyTombstone))
	}
	return d.appendV1RecordLocked(recordDelete, key, nil)
}

func (d *DiskStorage) appendLegacyRecordLocked(recType recordType, key types.Key, value types.Value) (diskIndexEntry, error) {
	size := recordSize(true, key, value)
	start := d.size
	if err := d.file.Truncate(d.size + size); err != nil {
		return diskIndexEntry{}, err
	}
	valueOffset, valueLen, err := writeRecordAt(d.file, true, start, recType, key, value)
	if err != nil {
		_ = d.file.Truncate(start)
		return diskIndexEntry{}, err
	}
	d.size += size

	if err := d.file.Sync(); err != nil {
		return diskIndexEntry{}, err
	}
	if err := d.remapFile(); err != nil {
		return diskIndexEntry{}, err
	}

	entry := diskIndexEntry{
		offset:   valueOffset,
		valueLen: valueLen,
		deleted:  recType == recordDelete || (valueLen == uint32(len(legacyTombstone)) && string(value) == legacyTombstone),
	}
	d.index[string(key)] = entry
	return entry, nil
}

func (d *DiskStorage) appendV1RecordLocked(recType recordType, key types.Key, value types.Value) (diskIndexEntry, error) {
	size := recordSize(false, key, value)
	start := d.size
	if err := d.file.Truncate(d.size + size); err != nil {
		return diskIndexEntry{}, err
	}
	valueOffset, valueLen, err := writeRecordAt(d.file, false, start, recType, key, value)
	if err != nil {
		_ = d.file.Truncate(start)
		return diskIndexEntry{}, err
	}
	d.size += size

	if err := d.file.Sync(); err != nil {
		return diskIndexEntry{}, err
	}
	if err := d.remapFile(); err != nil {
		return diskIndexEntry{}, err
	}

	entry := diskIndexEntry{
		offset:   valueOffset,
		valueLen: valueLen,
		deleted:  recType == recordDelete,
	}
	d.index[string(key)] = entry
	return entry, nil
}

func (d *DiskStorage) validateTransactionLocked(t *transaction.Transaction) error {
	exists := make(map[string]bool, len(t.Operations))
	initialized := make(map[string]bool, len(t.Operations))

	for _, op := range t.Operations {
		key := string(op.Key)
		if !initialized[key] {
			entry, ok := d.index[key]
			exists[key] = ok && !entry.deleted
			initialized[key] = true
		}

		switch op.Type {
		case types.Put:
			exists[key] = true
		case types.Delete:
			if !exists[key] {
				return ErrKeyNotFound
			}
			exists[key] = false
		default:
			return fmt.Errorf("%w: invalid operation type", ErrCorruptData)
		}
	}

	return nil
}

func (d *DiskStorage) snapshotIndexLocked(t *transaction.Transaction) map[string]diskIndexEntry {
	previous := make(map[string]diskIndexEntry, len(t.Operations))
	for _, op := range t.Operations {
		if entry, ok := d.index[string(op.Key)]; ok {
			previous[string(op.Key)] = entry
		} else {
			previous[string(op.Key)] = diskIndexEntry{deleted: true}
		}
	}
	return previous
}

func (d *DiskStorage) restoreIndexLocked(previous map[string]diskIndexEntry) {
	for key, entry := range previous {
		if entry.deleted && entry.offset == 0 && entry.valueLen == 0 {
			delete(d.index, key)
			continue
		}
		d.index[key] = entry
	}
}

func (d *DiskStorage) applyLegacyTransactionLocked(t *transaction.Transaction) error {
	totalSize := int64(0)
	for _, op := range t.Operations {
		value := op.Value
		if op.Type == types.Delete {
			value = []byte(legacyTombstone)
		}
		totalSize += recordSize(true, op.Key, value)
	}

	start := d.size
	if err := d.file.Truncate(d.size + totalSize); err != nil {
		return err
	}

	offset := start
	for _, op := range t.Operations {
		value := op.Value
		recType := recordPut
		if op.Type == types.Delete {
			value = []byte(legacyTombstone)
			recType = recordDelete
		}
		valueOffset, valueLen, err := writeRecordAt(d.file, true, offset, recType, op.Key, value)
		if err != nil {
			return err
		}
		d.index[string(op.Key)] = diskIndexEntry{
			offset:   valueOffset,
			valueLen: valueLen,
			deleted:  recType == recordDelete || (valueLen == uint32(len(legacyTombstone)) && string(value) == legacyTombstone),
		}
		offset += recordSize(true, op.Key, value)
	}

	d.size += totalSize
	if err := d.file.Sync(); err != nil {
		return err
	}
	return d.remapFile()
}

func (d *DiskStorage) applyV1TransactionLocked(t *transaction.Transaction) error {
	totalSize := int64(0)
	for _, op := range t.Operations {
		value := op.Value
		if op.Type == types.Delete {
			value = nil
		}
		totalSize += recordSize(false, op.Key, value)
	}

	start := d.size
	if err := d.file.Truncate(d.size + totalSize); err != nil {
		return err
	}

	offset := start
	for _, op := range t.Operations {
		recType := recordPut
		value := op.Value
		if op.Type == types.Delete {
			recType = recordDelete
			value = nil
		}

		valueOffset, valueLen, err := writeRecordAt(d.file, false, offset, recType, op.Key, value)
		if err != nil {
			return err
		}
		d.index[string(op.Key)] = diskIndexEntry{
			offset:   valueOffset,
			valueLen: valueLen,
			deleted:  recType == recordDelete,
		}
		offset += recordSize(false, op.Key, value)
	}

	d.size += totalSize
	if err := d.file.Sync(); err != nil {
		return err
	}
	return d.remapFile()
}
