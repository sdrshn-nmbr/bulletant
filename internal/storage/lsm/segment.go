package lsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

const (
	segmentMagic      = "BLSM"
	segmentVersion    = uint8(1)
	segmentHeaderSize = 44
)

const (
	segmentOpPut    = byte(1)
	segmentOpDelete = byte(2)
)

type segmentHeader struct {
	Entries     uint32
	DataOffset  uint64
	BloomOffset uint64
	IndexOffset uint64
	BloomBytes  uint32
	IndexBytes  uint32
}

type segmentEntry struct {
	key       string
	value     []byte
	tombstone bool
}

type indexEntry struct {
	key       string
	offset    uint64
	valueLen  uint32
	tombstone bool
}

func writeSegmentHeader(file *os.File, header segmentHeader) error {
	buf := &bytes.Buffer{}
	buf.WriteString(segmentMagic)
	buf.WriteByte(segmentVersion)
	buf.Write([]byte{0, 0, 0})
	if err := binary.Write(buf, binary.LittleEndian, header.Entries); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, header.DataOffset); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, header.BloomOffset); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, header.IndexOffset); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, header.BloomBytes); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, header.IndexBytes); err != nil {
		return err
	}

	data := buf.Bytes()
	if len(data) != segmentHeaderSize {
		return io.ErrShortWrite
	}
	_, err := file.WriteAt(data, 0)
	return err
}

func readSegmentHeader(file *os.File) (segmentHeader, error) {
	buf := make([]byte, segmentHeaderSize)
	if _, err := file.ReadAt(buf, 0); err != nil {
		return segmentHeader{}, err
	}
	if string(buf[:4]) != segmentMagic {
		return segmentHeader{}, storage.ErrCorruptSegment
	}
	if buf[4] != segmentVersion {
		return segmentHeader{}, storage.ErrCorruptSegment
	}

	reader := bytes.NewReader(buf[8:])
	var header segmentHeader
	if err := binary.Read(reader, binary.LittleEndian, &header.Entries); err != nil {
		return segmentHeader{}, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &header.DataOffset); err != nil {
		return segmentHeader{}, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &header.BloomOffset); err != nil {
		return segmentHeader{}, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &header.IndexOffset); err != nil {
		return segmentHeader{}, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &header.BloomBytes); err != nil {
		return segmentHeader{}, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &header.IndexBytes); err != nil {
		return segmentHeader{}, err
	}
	return header, nil
}

func segmentDataEntrySize(key string, value []byte) uint64 {
	return uint64(1 + 4 + 4 + len(key) + len(value))
}

func segmentIndexEntrySize(key string) uint64 {
	return uint64(4 + len(key) + 8 + 4 + 1)
}
