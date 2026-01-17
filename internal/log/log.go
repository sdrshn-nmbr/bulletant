package log

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type recordType uint8

const (
	recordBegin recordType = iota + 1
	recordCommit
	recordAbort
)

type WAL struct {
	file *os.File
	mu   sync.Mutex
}

func NewWAL(filename string) (*WAL, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return &WAL{file: file}, nil
}

func (w *WAL) LogBegin(t *transaction.Transaction) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	ops := make([]transaction.Operation, 0, len(t.Operations))
	for _, op := range t.Operations {
		if op.Type == types.Put || op.Type == types.Delete {
			ops = append(ops, op)
		}
	}

	if err := w.writeRecordType(recordBegin); err != nil {
		return err
	}
	if _, err := w.file.Write(t.ID[:]); err != nil {
		return err
	}
	if err := binary.Write(w.file, binary.LittleEndian, uint32(len(ops))); err != nil {
		return err
	}

	for _, op := range ops {
		if err := binary.Write(w.file, binary.LittleEndian, uint8(op.Type)); err != nil {
			return err
		}
		if err := binary.Write(w.file, binary.LittleEndian, uint32(len(op.Key))); err != nil {
			return err
		}
		if _, err := w.file.Write([]byte(op.Key)); err != nil {
			return err
		}
		if err := binary.Write(w.file, binary.LittleEndian, uint32(len(op.Value))); err != nil {
			return err
		}
		if len(op.Value) > 0 {
			if _, err := w.file.Write([]byte(op.Value)); err != nil {
				return err
			}
		}
	}

	return w.file.Sync()
}

func (w *WAL) LogCommit(id uuid.UUID) error {
	return w.logStatus(recordCommit, id)
}

func (w *WAL) LogAbort(id uuid.UUID) error {
	return w.logStatus(recordAbort, id)
}

func (w *WAL) LogTransaction(t *transaction.Transaction) error {
	if err := w.LogBegin(t); err != nil {
		return err
	}
	return w.LogCommit(t.ID)
}

func (w *WAL) Recover(storage storage.Storage) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	pending := make(map[uuid.UUID]*transaction.Transaction)
	for {
		recType, err := w.readRecordType()
		if isEOF(err) {
			break
		}
		if err != nil {
			return err
		}

		switch recType {
		case recordBegin:
			txn, err := w.readBeginRecord()
			if err != nil {
				if isEOF(err) {
					return nil
				}
				return err
			}
			pending[txn.ID] = txn
		case recordCommit:
			id, err := w.readUUID()
			if err != nil {
				if isEOF(err) {
					return nil
				}
				return err
			}
			txn, ok := pending[id]
			if !ok {
				continue
			}
			if err := storage.ExecuteTransaction(txn); err != nil {
				return err
			}
			delete(pending, id)
		case recordAbort:
			id, err := w.readUUID()
			if err != nil {
				if isEOF(err) {
					return nil
				}
				return err
			}
			delete(pending, id)
		default:
			return errors.New("unknown WAL record type")
		}
	}

	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Close()
}

func (w *WAL) logStatus(rt recordType, id uuid.UUID) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writeRecordType(rt); err != nil {
		return err
	}
	if _, err := w.file.Write(id[:]); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) writeRecordType(rt recordType) error {
	return binary.Write(w.file, binary.LittleEndian, uint8(rt))
}

func (w *WAL) readRecordType() (recordType, error) {
	var value uint8
	if err := binary.Read(w.file, binary.LittleEndian, &value); err != nil {
		return 0, err
	}
	return recordType(value), nil
}

func (w *WAL) readUUID() (uuid.UUID, error) {
	buf := make([]byte, 16)
	if _, err := io.ReadFull(w.file, buf); err != nil {
		return uuid.UUID{}, err
	}
	return uuid.FromBytes(buf)
}

func (w *WAL) readBeginRecord() (*transaction.Transaction, error) {
	id, err := w.readUUID()
	if err != nil {
		return nil, err
	}

	var numOps uint32
	if err := binary.Read(w.file, binary.LittleEndian, &numOps); err != nil {
		return nil, err
	}

	txn := transaction.NewTransaction()
	txn.ID = id

	for i := uint32(0); i < numOps; i++ {
		var opType uint8
		if err := binary.Read(w.file, binary.LittleEndian, &opType); err != nil {
			return nil, err
		}

		var keyLen uint32
		if err := binary.Read(w.file, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(w.file, key); err != nil {
			return nil, err
		}

		var valueLen uint32
		if err := binary.Read(w.file, binary.LittleEndian, &valueLen); err != nil {
			return nil, err
		}

		value := make([]byte, valueLen)
		if valueLen > 0 {
			if _, err := io.ReadFull(w.file, value); err != nil {
				return nil, err
			}
		}

		switch types.OperationType(opType) {
		case types.Put:
			txn.Put(types.Key(key), types.Value(value))
		case types.Delete:
			txn.Delete(types.Key(key))
		default:
			return nil, errors.New("invalid operation type in WAL")
		}
	}

	return txn, nil
}

func isEOF(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
