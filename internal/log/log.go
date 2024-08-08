package log

import (
	"encoding/binary"
	"os"

	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type WAL struct {
	file *os.File
}

func NewWAL(filename string) (*WAL, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return &WAL{file: file}, nil
}

func (w *WAL) LogTransaction(t *transaction.Transaction) error {
	// Write transaction to log
	// Format: [number of ops (uint32)][op type (types.OperationType)][key length (uint32)][key (types.Key)][value length (uint32)][value (types.Value)]

	// [number of ops (uint32)]
	if err := binary.Write(w.file, binary.LittleEndian, uint32(len(t.Operations))); err != nil {
		return err
	}

	for _, op := range t.Operations {
		// Only 1 (Put) or 2 (Delete) written, as they actually change the data
		if op.Type == types.Put || op.Type == types.Delete {
			// [op type (types.OperationType)]
			if err := binary.Write(w.file, binary.LittleEndian, uint8(op.Type)); err != nil {
				return err
			}

			// [key length (uint32)]
			if err := binary.Write(w.file, binary.LittleEndian, uint32(len(op.Key))); err != nil {
				return err
			}

			// [key (types.Key)]
			if _, err := w.file.Write([]byte(op.Key)); err != nil {
				return err
			}

			if op.Type == types.Put {
				// [value length (uint32)]
				if err := binary.Write(w.file, binary.LittleEndian, uint32(len(op.Value))); err != nil {
					return err
				}

				// [value (types.Value)]
				if _, err := w.file.Write([]byte(op.Value)); err != nil {
					return err
				}
			}
		}

	}

	return w.file.Sync()
}

// ! Implement recovery method