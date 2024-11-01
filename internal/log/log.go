package log

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
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
func (w *WAL) Recover(storage storage.Storage) error {
	_, err := w.file.Seek(0, 0)
	if err != nil {
		return err
	}

	for {
		t, err := w.readTransaction()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		err = storage.ExecuteTransaction(t)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *WAL) readTransaction() (*transaction.Transaction, error) {
	var numOps uint32
	err := binary.Read(w.file, binary.LittleEndian, &numOps)
	if err != nil {
		return nil, err
	}

	t := transaction.NewTransaction()

	for i := uint32(0); i < numOps; i++ {
		var opType uint8
		err = binary.Read(w.file, binary.LittleEndian, &opType)
		if err != nil {
			return nil, err
		}

		var keyLen uint32
		err = binary.Read(w.file, binary.LittleEndian, &keyLen)
		if err != nil {
			return nil, err
		}

		key := make([]byte, keyLen)
		_, err = w.file.Read(key)
		if err != nil {
			return nil, err
		}

		if types.OperationType(opType) == types.Put {
			var valueLen uint32
			err = binary.Read(w.file, binary.LittleEndian, &valueLen)
			if err != nil {
				return nil, err
			}

			value := make([]byte, valueLen)
			_, err = w.file.Read(value)
			if err != nil {
				return nil, err
			}

			t.Put(types.Key(key), types.Value(value))
		} else if types.OperationType(opType) == types.Delete {
			t.Delete(types.Key(key))
		}
	}

	return t, nil
}
