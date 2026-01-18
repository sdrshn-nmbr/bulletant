package lsm

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

func writeSegment(
	path string,
	entries []segmentEntry,
	bitsPerKey float64,
	limiter storage.RateLimiter,
	ctx context.Context,
) (segmentMeta, error) {
	if len(entries) == 0 {
		return segmentMeta{}, storage.ErrInvalidCompactLimit
	}
	if bitsPerKey <= 0 {
		return segmentMeta{}, storage.ErrInvalidBloomParams
	}
	if ctx == nil {
		ctx = context.Background()
	}

	sort.Slice(entries, func(i int, j int) bool {
		return entries[i].key < entries[j].key
	})

	filter, err := newBloomFilter(uint32(len(entries)), bitsPerKey)
	if err != nil {
		return segmentMeta{}, err
	}

	tempPath := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return segmentMeta{}, err
	}
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		return segmentMeta{}, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()

	if _, err := file.Write(make([]byte, segmentHeaderSize)); err != nil {
		file.Close()
		return segmentMeta{}, err
	}

	offset := int64(segmentHeaderSize)
	index := make([]indexEntry, 0, len(entries))
	minKey := entries[0].key
	maxKey := entries[len(entries)-1].key

	for _, entry := range entries {
		if ctx != nil && ctx.Err() != nil {
			file.Close()
			return segmentMeta{}, ctx.Err()
		}

		keyBytes := []byte(entry.key)
		value := entry.value
		op := segmentOpPut
		if entry.tombstone {
			value = nil
			op = segmentOpDelete
		}

		filter.add(keyBytes)
		if err := writeSegmentEntry(file, &offset, op, keyBytes, value); err != nil {
			file.Close()
			return segmentMeta{}, err
		}

		valueOffset := uint64(offset) - uint64(len(value))
		index = append(index, indexEntry{
			key:       entry.key,
			offset:    valueOffset,
			valueLen:  uint32(len(value)),
			tombstone: entry.tombstone,
		})

		if limiter != nil {
			size := segmentDataEntrySize(entry.key, value)
			if err := limiter.WaitN(ctx, int64(size)); err != nil {
				file.Close()
				return segmentMeta{}, err
			}
		}
	}

	bloomOffset := uint64(offset)
	bloomBytes := filter.marshal()
	if len(bloomBytes) > 0 {
		if _, err := file.WriteAt(bloomBytes, int64(bloomOffset)); err != nil {
			file.Close()
			return segmentMeta{}, err
		}
		offset += int64(len(bloomBytes))
		if limiter != nil {
			if err := limiter.WaitN(ctx, int64(len(bloomBytes))); err != nil {
				file.Close()
				return segmentMeta{}, err
			}
		}
	}

	indexOffset := uint64(offset)
	indexStart := offset
	for _, entry := range index {
		if err := writeIndexEntry(file, &offset, entry); err != nil {
			file.Close()
			return segmentMeta{}, err
		}
		if limiter != nil {
			size := segmentIndexEntrySize(entry.key)
			if err := limiter.WaitN(ctx, int64(size)); err != nil {
				file.Close()
				return segmentMeta{}, err
			}
		}
	}
	indexBytes := uint32(offset - indexStart)

	header := segmentHeader{
		Entries:     uint32(len(entries)),
		DataOffset:  uint64(segmentHeaderSize),
		BloomOffset: bloomOffset,
		IndexOffset: indexOffset,
		BloomBytes:  uint32(len(bloomBytes)),
		IndexBytes:  indexBytes,
	}
	if err := writeSegmentHeader(file, header); err != nil {
		file.Close()
		return segmentMeta{}, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return segmentMeta{}, err
	}
	if err := file.Close(); err != nil {
		return segmentMeta{}, err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return segmentMeta{}, err
	}
	cleanup = false

	return segmentMeta{
		Level:    0,
		MinKey:   minKey,
		MaxKey:   maxKey,
		Entries:  uint32(len(entries)),
		Bytes:    uint64(offset),
		Filename: filepath.Base(path),
	}, nil
}

func writeSegmentEntry(file *os.File, offset *int64, op byte, key []byte, value []byte) error {
	if _, err := file.WriteAt([]byte{op}, *offset); err != nil {
		return err
	}
	*offset++

	if err := writeUint32(file, offset, uint32(len(key))); err != nil {
		return err
	}
	if err := writeUint32(file, offset, uint32(len(value))); err != nil {
		return err
	}
	if len(key) > 0 {
		if _, err := file.WriteAt(key, *offset); err != nil {
			return err
		}
		*offset += int64(len(key))
	}
	if len(value) > 0 {
		if _, err := file.WriteAt(value, *offset); err != nil {
			return err
		}
		*offset += int64(len(value))
	}
	return nil
}

func writeIndexEntry(file *os.File, offset *int64, entry indexEntry) error {
	if err := writeUint32(file, offset, uint32(len(entry.key))); err != nil {
		return err
	}
	if len(entry.key) > 0 {
		if _, err := file.WriteAt([]byte(entry.key), *offset); err != nil {
			return err
		}
		*offset += int64(len(entry.key))
	}
	if err := writeUint64(file, offset, entry.offset); err != nil {
		return err
	}
	if err := writeUint32(file, offset, entry.valueLen); err != nil {
		return err
	}
	op := segmentOpPut
	if entry.tombstone {
		op = segmentOpDelete
	}
	if _, err := file.WriteAt([]byte{op}, *offset); err != nil {
		return err
	}
	*offset++
	return nil
}

func writeUint32(file *os.File, offset *int64, value uint32) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, value)
	if _, err := file.WriteAt(buf, *offset); err != nil {
		return err
	}
	*offset += 4
	return nil
}

func writeUint64(file *os.File, offset *int64, value uint64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, value)
	if _, err := file.WriteAt(buf, *offset); err != nil {
		return err
	}
	*offset += 8
	return nil
}
