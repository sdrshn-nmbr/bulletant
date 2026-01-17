package storage

import (
	"bytes"
	"encoding/binary"
	"os"
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

