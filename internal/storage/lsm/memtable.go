package lsm

import "sort"

type memEntry struct {
	value     []byte
	tombstone bool
	size      uint64
}

type memtable struct {
	entries map[string]memEntry
	bytes   uint64
	count   uint32
}

func newMemtable() *memtable {
	return &memtable{
		entries: make(map[string]memEntry),
	}
}

func (m *memtable) get(key string) (memEntry, bool) {
	entry, ok := m.entries[key]
	return entry, ok
}

func (m *memtable) set(key string, value []byte, tombstone bool) {
	entrySize := memEntrySize(key, value)
	if existing, ok := m.entries[key]; ok {
		m.bytes -= existing.size
	} else {
		m.count++
	}

	valueCopy := append([]byte(nil), value...)
	m.entries[key] = memEntry{
		value:     valueCopy,
		tombstone: tombstone,
		size:      entrySize,
	}
	m.bytes += entrySize
}

func (m *memtable) reset() {
	m.entries = make(map[string]memEntry)
	m.bytes = 0
	m.count = 0
}

func (m *memtable) entriesSnapshot() []segmentEntry {
	entries := make([]segmentEntry, 0, len(m.entries))
	for key, entry := range m.entries {
		valueCopy := append([]byte(nil), entry.value...)
		entries = append(entries, segmentEntry{
			key:       key,
			value:     valueCopy,
			tombstone: entry.tombstone,
		})
	}
	sort.Slice(entries, func(i int, j int) bool {
		return entries[i].key < entries[j].key
	})
	return entries
}

func memEntrySize(key string, value []byte) uint64 {
	return uint64(len(key)+len(value)) + 16
}
