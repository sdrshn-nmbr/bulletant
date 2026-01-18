package lsm

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"math"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

type bloomFilter struct {
	bits []byte
	m    uint64
	k    uint8
}

func newBloomFilter(keys uint32, bitsPerKey float64) (*bloomFilter, error) {
	if bitsPerKey <= 0 {
		return nil, storage.ErrInvalidBloomParams
	}
	if keys == 0 {
		return &bloomFilter{
			bits: make([]byte, 8),
			m:    64,
			k:    1,
		}, nil
	}

	m := uint64(math.Ceil(float64(keys) * bitsPerKey))
	if m < 64 {
		m = 64
	}
	k := uint8(math.Round((float64(m) / float64(keys)) * math.Ln2))
	if k == 0 {
		k = 1
	}
	bytesLen := (m + 7) / 8
	return &bloomFilter{
		bits: make([]byte, bytesLen),
		m:    m,
		k:    k,
	}, nil
}

func (b *bloomFilter) add(key []byte) {
	if b == nil || b.m == 0 || b.k == 0 {
		return
	}
	h1, h2 := bloomHashes(key)
	for i := uint8(0); i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % b.m
		b.bits[pos/8] |= 1 << (pos % 8)
	}
}

func (b *bloomFilter) mayContain(key []byte) bool {
	if b == nil || b.m == 0 || b.k == 0 {
		return true
	}
	h1, h2 := bloomHashes(key)
	for i := uint8(0); i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % b.m
		if b.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

func (b *bloomFilter) marshal() []byte {
	if b == nil {
		return nil
	}
	buf := &bytes.Buffer{}
	buf.WriteByte(b.k)
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(b.bits)))
	buf.Write(b.bits)
	return buf.Bytes()
}

func unmarshalBloom(data []byte) (*bloomFilter, error) {
	if len(data) < 5 {
		return nil, storage.ErrCorruptSegment
	}
	k := data[0]
	bitLen := binary.LittleEndian.Uint32(data[1:5])
	if len(data) < int(5+bitLen) {
		return nil, storage.ErrCorruptSegment
	}
	bits := make([]byte, bitLen)
	copy(bits, data[5:5+bitLen])
	m := uint64(bitLen) * 8
	if k == 0 || m == 0 {
		return nil, storage.ErrCorruptSegment
	}
	return &bloomFilter{
		bits: bits,
		m:    m,
		k:    k,
	}, nil
}

func bloomHashes(key []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	_, _ = h1.Write(key)
	h2 := fnv.New64()
	_, _ = h2.Write(key)
	return h1.Sum64(), h2.Sum64()
}
