package storage

import (
	"sort"

	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

func buildScanResult(entries []ScanEntry, req ScanRequest) ScanResult {
	sort.Slice(entries, func(i int, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	cursor := string(req.Cursor)
	start := scanStartIndex(entriesToKeys(entries), cursor)
	if start >= len(entries) {
		return ScanResult{Entries: []ScanEntry{}}
	}

	limit := int(req.Limit)
	end := start + limit
	if end > len(entries) {
		end = len(entries)
	}

	selected := entries[start:end]
	nextCursor := types.Key("")
	if end < len(entries) {
		nextCursor = selected[len(selected)-1].Key
	}

	return ScanResult{
		Entries:    selected,
		NextCursor: nextCursor,
	}
}

func entriesToKeys(entries []ScanEntry) []string {
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = string(entry.Key)
	}
	return keys
}

