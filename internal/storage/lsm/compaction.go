package lsm

import (
	"context"
	"sort"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
)

func (s *LSMStorage) compactWithOptions(opts storage.CompactOptions) (storage.CompactStats, error) {
	stats := storage.CompactStats{}
	if opts.Context == nil {
		opts.Context = context.Background()
	}
	if opts.MaxEntries == 0 || opts.MaxBytes == 0 {
		return stats, storage.ErrInvalidCompactLimit
	}

	s.compactionMu.Lock()
	defer s.compactionMu.Unlock()

	s.mu.RLock()
	segments := append([]*segmentReader(nil), s.segments...)
	s.mu.RUnlock()

	stats.BytesBefore = sumSegmentBytes(segments)
	stats.EntriesTotal = sumSegmentEntries(segments)

	targets := selectCompactionTargets(segments, s.opts.CompactionMaxSegments)
	if len(targets) < 2 {
		return stats, nil
	}

	sort.Slice(targets, func(i, j int) bool {
		return targets[i].meta.ID > targets[j].meta.ID
	})

	iterators := make([]entryIterator, 0, len(targets))
	for _, seg := range targets {
		iterators = append(iterators, newSegmentIterator(seg, true, ^uint32(0), ""))
	}

	var newSegments []segmentMeta
	var pending []segmentEntry
	var pendingSize uint64
	var pendingEntries uint32
	totalBytes := uint64(0)
	totalEntries := uint32(0)

	flushPending := func() error {
		if len(pending) == 0 {
			return nil
		}
		id := s.nextSegmentID()
		path := segmentPath(s.dir, segmentFileName(id))
		meta, err := writeSegment(path, pending, s.opts.BloomBitsPerKey, opts.RateLimiter, opts.Context)
		if err != nil {
			return err
		}
		meta.ID = id
		newSegments = append(newSegments, meta)
		totalBytes += meta.Bytes
		totalEntries += meta.Entries
		pending = nil
		pendingSize = 0
		pendingEntries = 0
		return nil
	}

	err := mergeForCompaction(iterators, func(entry segmentEntry) error {
		entrySize := segmentDataEntrySize(entry.key, entry.value) + segmentIndexEntrySize(entry.key)
		if totalEntries+pendingEntries+1 > opts.MaxEntries {
			return storage.ErrCompactLimitExceeded
		}
		if totalBytes+pendingSize+entrySize > opts.MaxBytes {
			return storage.ErrCompactLimitExceeded
		}
		if s.opts.SegmentMaxBytes > 0 && pendingSize+entrySize > s.opts.SegmentMaxBytes && len(pending) > 0 {
			if err := flushPending(); err != nil {
				return err
			}
		}
		pending = append(pending, entry)
		pendingSize += entrySize
		pendingEntries++
		return nil
	})
	if err != nil {
		return stats, err
	}
	if err := flushPending(); err != nil {
		return stats, err
	}

	stats.BytesAfter = totalBytes
	stats.EntriesWritten = totalEntries

	if err := s.replaceSegments(targets, newSegments); err != nil {
		return stats, err
	}
	return stats, nil
}

func selectCompactionTargets(segments []*segmentReader, maxSegments uint32) []*segmentReader {
	if len(segments) == 0 {
		return nil
	}
	if maxSegments == 0 || int(maxSegments) > len(segments) {
		maxSegments = uint32(len(segments))
	}
	start := len(segments) - int(maxSegments)
	return append([]*segmentReader(nil), segments[start:]...)
}

func sumSegmentBytes(segments []*segmentReader) uint64 {
	var total uint64
	for _, seg := range segments {
		total += seg.meta.Bytes
	}
	return total
}

func sumSegmentEntries(segments []*segmentReader) uint32 {
	var total uint32
	for _, seg := range segments {
		total += seg.meta.Entries
	}
	return total
}
