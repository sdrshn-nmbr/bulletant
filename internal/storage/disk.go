package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"

	"github.com/sdrshn-nmbr/bulletant/internal/types"
	"golang.org/x/exp/mmap"
)

type DiskStorage struct {
	file *os.File
	mmap *mmap.ReaderAt
	size int64
	mu   sync.RWMutex
}

const TOMBSTONE string = "DELETED_DATA"

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

	mmapFile, err := mmap.Open(filename)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &DiskStorage{
		file: file,
		mmap: mmapFile,
		size: info.Size(),
	}, nil
}

func (d *DiskStorage) Put(key types.Key, value types.Value) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Format: [key length][key][value length][value]
	entrySize := 4 + len(key) + 4 + len(value)

	// Grow the file if necessary
	if int64(entrySize) > d.size {
		err := d.growFile(int64(entrySize))
		if err != nil {
			return err
		}
	}

	// Write to the end of the file
	offset := d.size
	d.size += int64(entrySize)

	// We need to write to the file directly, as mmap is read-only
	_, err := d.file.WriteAt(uint32ToBytes(uint32(len(key))), offset)
	if err != nil {
		return err
	}
	offset += 4

	_, err = d.file.WriteAt([]byte(key), offset)
	if err != nil {
		return err
	}
	offset += int64(len(key))

	_, err = d.file.WriteAt(uint32ToBytes(uint32(len(value))), offset)
	if err != nil {
		return err
	}
	offset += 4

	_, err = d.file.WriteAt(value, offset)
	if err != nil {
		return err
	}

	// Re-map the file after writing
	return d.remapFile()
}

func (d *DiskStorage) Get(key types.Key) (types.Value, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var offset int64 = 0
	for offset < d.size {
		keyLenBytes := make([]byte, 4)
		_, err := d.mmap.ReadAt(keyLenBytes, offset)
		if err != nil {
			return nil, err
		}
		keyLen := binary.LittleEndian.Uint32(keyLenBytes)
		offset += 4

		readKey := make([]byte, keyLen)
		_, err = d.mmap.ReadAt(readKey, offset)
		if err != nil {
			return nil, err
		}
		offset += int64(keyLen)

		valLenBytes := make([]byte, 4)
		_, err = d.mmap.ReadAt(valLenBytes, offset)
		if err != nil {
			return nil, err
		}
		valLen := binary.LittleEndian.Uint32(valLenBytes)
		offset += 4

		if string(key) == string(readKey) {
			value := make([]byte, valLen)
			_, err = d.mmap.ReadAt(value, offset)
			if err != nil {
				return nil, err
			}

			if string(value) == TOMBSTONE {
				return nil, errors.New("key does not exist - has been deleted previously")
			}

			return value, nil
		}

		offset += int64(valLen)
	}

	return nil, errors.New("key does not exist")
}

func (d *DiskStorage) Delete(key types.Key) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var offset int64 = 0
	for offset < d.size {
		keyLenBytes := make([]byte, 4)
		_, err := d.mmap.ReadAt(keyLenBytes, offset)
		if err != nil {
			return err
		}
		keyLen := binary.LittleEndian.Uint32(keyLenBytes)
		offset += 4

		readKey := make([]byte, keyLen)
		_, err = d.mmap.ReadAt(readKey, offset)
		if err != nil {
			return err
		}
		offset += int64(keyLen)

		valLenBytes := make([]byte, 4)
		_, err = d.mmap.ReadAt(valLenBytes, offset)
		if err != nil {
			return err
		}
		valLen := binary.LittleEndian.Uint32(valLenBytes)
		offset += 4

		if string(key) == string(readKey) {
			tombstoneData := []byte(TOMBSTONE)
			if int(valLen) > len(tombstoneData) {
				tombstoneData = append(tombstoneData, make([]byte, int(valLen)-len(tombstoneData))...)
			} else {
				tombstoneData = tombstoneData[:valLen]
			}

			_, err = d.file.WriteAt(tombstoneData, offset)
			if err != nil {
				return err
			}

			// Re-map the file after writing
			return d.remapFile()
		}

		offset += int64(valLen)
	}

	return errors.New("key does not exist")
}

func (d *DiskStorage) growFile(additionalSize int64) error {
	err := d.file.Truncate(d.size + additionalSize)
	if err != nil {
		return err
	}
	return d.remapFile()
}

func (d *DiskStorage) remapFile() error {
	// Close the existing mmap
	if err := d.mmap.Close(); err != nil {
		return err
	}

	// Re-open the mmap
	mmapFile, err := mmap.Open(d.file.Name())
	if err != nil {
		return err
	}
	d.mmap = mmapFile

	// Update the size
	info, err := d.file.Stat()
	if err != nil {
		return err
	}
	d.size = info.Size()

	return nil
}

func (d *DiskStorage) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.mmap.Close(); err != nil {
		return err
	}
	return d.file.Close()
}

func uint32ToBytes(u uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, u)
	return b
}
