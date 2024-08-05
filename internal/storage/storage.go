package storage

import (
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type Storage interface {
	Get(key types.Key) (types.Value, error)
	Put(key types.Key, value types.Value) error
	Delete (key types.Key) error
}
