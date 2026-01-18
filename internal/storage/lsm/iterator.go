package lsm

import (
	"container/heap"
	"sort"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type entryRecord struct {
	key       string
	value     []byte
	valueLen  uint32
	tombstone bool
}

type entryIterator interface {
	Next() bool
	Entry() entryRecord
	Err() error
}

type memIterator struct {
	entries       []segmentEntry
	pos           int
	includeValues bool
	maxValueBytes uint32
	err           error
	current       entryRecord
}

func newMemIterator(entries []segmentEntry, includeValues bool, maxValueBytes uint32, cursor string) *memIterator {
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].key > cursor
	})
	return &memIterator{
		entries:       entries,
		pos:           start - 1,
		includeValues: includeValues,
		maxValueBytes: maxValueBytes,
	}
}

func (m *memIterator) Next() bool {
	if m.err != nil {
		return false
	}
	m.pos++
	if m.pos >= len(m.entries) {
		return false
	}
	entry := m.entries[m.pos]
	value := entry.value
	if !m.includeValues {
		value = nil
	}
	if m.includeValues && !entry.tombstone {
		if uint32(len(entry.value)) > m.maxValueBytes {
			m.err = storage.ErrValueTooLarge
			return false
		}
	}
	m.current = entryRecord{
		key:       entry.key,
		value:     value,
		valueLen:  uint32(len(entry.value)),
		tombstone: entry.tombstone,
	}
	return true
}

func (m *memIterator) Entry() entryRecord {
	return m.current
}

func (m *memIterator) Err() error {
	return m.err
}

type segmentIterator struct {
	reader        *segmentReader
	pos           int
	includeValues bool
	maxValueBytes uint32
	err           error
	current       entryRecord
}

func newSegmentIterator(reader *segmentReader, includeValues bool, maxValueBytes uint32, cursor string) *segmentIterator {
	start := sort.Search(len(reader.index), func(i int) bool {
		return reader.index[i].key > cursor
	})
	return &segmentIterator{
		reader:        reader,
		pos:           start - 1,
		includeValues: includeValues,
		maxValueBytes: maxValueBytes,
	}
}

func (s *segmentIterator) Next() bool {
	if s.err != nil {
		return false
	}
	s.pos++
	if s.pos >= len(s.reader.index) {
		return false
	}
	entry := s.reader.index[s.pos]
	value := []byte(nil)
	if !entry.tombstone && s.includeValues {
		if entry.valueLen > s.maxValueBytes {
			s.err = storage.ErrValueTooLarge
			return false
		}
		if entry.valueLen > 0 {
			value = make([]byte, entry.valueLen)
			if _, err := s.reader.file.ReadAt(value, int64(entry.offset)); err != nil {
				s.err = err
				return false
			}
		} else {
			value = []byte{}
		}
	}
	s.current = entryRecord{
		key:       entry.key,
		value:     value,
		valueLen:  entry.valueLen,
		tombstone: entry.tombstone,
	}
	return true
}

func (s *segmentIterator) Entry() entryRecord {
	return s.current
}

func (s *segmentIterator) Err() error {
	return s.err
}

type mergeItem struct {
	key  string
	rank int
	iter entryIterator
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if h[i].key == h[j].key {
		return h[i].rank < h[j].rank
	}
	return h[i].key < h[j].key
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func mergeForScan(
	iterators []entryIterator,
	limit uint32,
	includeValues bool,
	maxValueBytes uint32,
	prefix string,
	cursor string,
) ([]storage.ScanEntry, string, error) {
	if limit == 0 {
		return []storage.ScanEntry{}, "", nil
	}
	entries := make([]storage.ScanEntry, 0, limit)
	if len(iterators) == 0 {
		return entries, "", nil
	}

	var prefixEnd string
	var hasPrefixEnd bool
	if prefix != "" {
		prefixEnd, hasPrefixEnd = nextPrefix(prefix)
	}

	h := &mergeHeap{}
	for rank, iter := range iterators {
		if iter.Next() {
			item := mergeItem{
				key:  iter.Entry().key,
				rank: rank,
				iter: iter,
			}
			heap.Push(h, item)
		} else if err := iter.Err(); err != nil {
			return nil, "", err
		}
	}

	var lastKey string
	for h.Len() > 0 && uint32(len(entries)) < limit {
		item := heap.Pop(h).(mergeItem)
		key := item.key
		if prefix != "" && hasPrefixEnd && key >= prefixEnd {
			break
		}

		group := []mergeItem{item}
		for h.Len() > 0 && (*h)[0].key == key {
			group = append(group, heap.Pop(h).(mergeItem))
		}

		chosen := group[0]
		chosenEntry := chosen.iter.Entry()
		if key > cursor && (prefix == "" || hasPrefix(key, prefix)) {
			if !chosenEntry.tombstone {
				value := chosenEntry.value
				if includeValues {
					if chosenEntry.valueLen > maxValueBytes {
						return nil, "", storage.ErrValueTooLarge
					}
				} else {
					value = nil
				}
				entries = append(entries, storage.ScanEntry{
					Key:   types.Key(key),
					Value: value,
				})
				lastKey = key
			}
		}

		for _, groupItem := range group {
			iter := groupItem.iter
			if iter.Next() {
				heap.Push(h, mergeItem{
					key:  iter.Entry().key,
					rank: groupItem.rank,
					iter: iter,
				})
			} else if err := iter.Err(); err != nil {
				return nil, "", err
			}
		}
	}

	nextCursor := ""
	if len(entries) == int(limit) && h.Len() > 0 {
		nextCursor = lastKey
	}
	return entries, nextCursor, nil
}

func mergeForCompaction(
	iterators []entryIterator,
	onEntry func(segmentEntry) error,
) error {
	if len(iterators) == 0 {
		return nil
	}

	h := &mergeHeap{}
	for rank, iter := range iterators {
		if iter.Next() {
			heap.Push(h, mergeItem{
				key:  iter.Entry().key,
				rank: rank,
				iter: iter,
			})
		} else if err := iter.Err(); err != nil {
			return err
		}
	}

	for h.Len() > 0 {
		item := heap.Pop(h).(mergeItem)
		key := item.key

		group := []mergeItem{item}
		for h.Len() > 0 && (*h)[0].key == key {
			group = append(group, heap.Pop(h).(mergeItem))
		}
		chosen := group[0]
		chosenEntry := chosen.iter.Entry()
		if !chosenEntry.tombstone {
			valueCopy := append([]byte(nil), chosenEntry.value...)
			if err := onEntry(segmentEntry{
				key:       key,
				value:     valueCopy,
				tombstone: false,
			}); err != nil {
				return err
			}
		}

		for _, groupItem := range group {
			iter := groupItem.iter
			if iter.Next() {
				heap.Push(h, mergeItem{
					key:  iter.Entry().key,
					rank: groupItem.rank,
					iter: iter,
				})
			} else if err := iter.Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

func hasPrefix(key string, prefix string) bool {
	if len(prefix) == 0 {
		return true
	}
	if len(key) < len(prefix) {
		return false
	}
	return key[:len(prefix)] == prefix
}

func nextPrefix(prefix string) (string, bool) {
	if prefix == "" {
		return "", false
	}
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xFF {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}
