package lsm

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

type segmentReader struct {
	meta   segmentMeta
	file   *os.File
	header segmentHeader
	bloom  *bloomFilter
	index  []indexEntry
	minKey string
	maxKey string
}

func openSegment(dir string, meta segmentMeta) (*segmentReader, error) {
	path := segmentPath(dir, meta.Filename)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	header, err := readSegmentHeader(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	bloom, err := readBloom(file, header)
	if err != nil {
		file.Close()
		return nil, err
	}
	index, err := readIndex(file, header)
	if err != nil {
		file.Close()
		return nil, err
	}

	reader := &segmentReader{
		meta:   meta,
		file:   file,
		header: header,
		bloom:  bloom,
		index:  index,
	}
	if len(index) > 0 {
		reader.minKey = index[0].key
		reader.maxKey = index[len(index)-1].key
	}
	return reader, nil
}

func (s *segmentReader) Close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *segmentReader) Get(key string) ([]byte, error) {
	entry, ok, err := s.lookupEntry(key)
	if err != nil {
		return nil, err
	}
	if !ok || entry.tombstone {
		return nil, storage.ErrKeyNotFound
	}
	return s.readValue(entry)
}

func (s *segmentReader) lookupEntry(key string) (indexEntry, bool, error) {
	if len(s.index) == 0 {
		return indexEntry{}, false, nil
	}
	if s.minKey != "" && key < s.minKey {
		return indexEntry{}, false, nil
	}
	if s.maxKey != "" && key > s.maxKey {
		return indexEntry{}, false, nil
	}
	if s.bloom != nil && !s.bloom.mayContain([]byte(key)) {
		return indexEntry{}, false, nil
	}

	idx := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].key >= key
	})
	if idx >= len(s.index) || s.index[idx].key != key {
		return indexEntry{}, false, nil
	}
	return s.index[idx], true, nil
}

func (s *segmentReader) readValue(entry indexEntry) ([]byte, error) {
	if entry.valueLen == 0 {
		return []byte{}, nil
	}
	value := make([]byte, entry.valueLen)
	if _, err := s.file.ReadAt(value, int64(entry.offset)); err != nil {
		return nil, err
	}
	return value, nil
}

func readBloom(file *os.File, header segmentHeader) (*bloomFilter, error) {
	if header.BloomBytes == 0 {
		return nil, nil
	}
	buf := make([]byte, header.BloomBytes)
	if _, err := file.ReadAt(buf, int64(header.BloomOffset)); err != nil {
		return nil, err
	}
	return unmarshalBloom(buf)
}

func readIndex(file *os.File, header segmentHeader) ([]indexEntry, error) {
	if header.IndexBytes == 0 {
		return nil, nil
	}
	buf := make([]byte, header.IndexBytes)
	if _, err := file.ReadAt(buf, int64(header.IndexOffset)); err != nil {
		return nil, err
	}

	reader := bytes.NewReader(buf)
	entries := make([]indexEntry, 0, header.Entries)
	for reader.Len() > 0 {
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}
		keyBytes := make([]byte, keyLen)
		if _, err := reader.Read(keyBytes); err != nil {
			return nil, err
		}
		var offset uint64
		if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
			return nil, err
		}
		var valueLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
			return nil, err
		}
		op, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		tombstone := op == segmentOpDelete
		entries = append(entries, indexEntry{
			key:       string(keyBytes),
			offset:    offset,
			valueLen:  valueLen,
			tombstone: tombstone,
		})
	}

	return entries, nil
}
