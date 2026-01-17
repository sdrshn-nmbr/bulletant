package storage

import (
	"sort"
	"strings"

	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type ScanRequest struct {
	Cursor        types.Key
	Prefix        types.Key
	Limit         uint32
	IncludeValues bool
	MaxValueBytes uint32
}

type ScanEntry struct {
	Key   types.Key
	Value types.Value
}

type ScanResult struct {
	Entries    []ScanEntry
	NextCursor types.Key
}

func (r ScanRequest) Validate() error {
	maxInt := int(^uint(0) >> 1)
	if r.Limit == 0 {
		return ErrInvalidScanLimit
	}
	if r.Limit > uint32(maxInt) {
		return ErrInvalidScanLimit
	}
	if r.IncludeValues {
		if r.MaxValueBytes == 0 {
			return ErrInvalidValueLimit
		}
		if r.MaxValueBytes > uint32(maxInt) {
			return ErrInvalidValueLimit
		}
	}
	return nil
}

func filterKeys(keys []string, prefix string) []string {
	if prefix == "" {
		return keys
	}

	filtered := make([]string, 0, len(keys))
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			filtered = append(filtered, key)
		}
	}
	return filtered
}

func sortKeys(keys []string) {
	sort.Strings(keys)
}

func scanStartIndex(keys []string, cursor string) int {
	if cursor == "" {
		return 0
	}

	return sort.Search(len(keys), func(i int) bool {
		return keys[i] > cursor
	})
}
