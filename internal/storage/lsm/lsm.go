package lsm

import (
	"context"
	"errors"
	"os"
	"sort"
	"sync"

	"github.com/sdrshn-nmbr/bulletant/internal/storage"
	"github.com/sdrshn-nmbr/bulletant/internal/transaction"
	"github.com/sdrshn-nmbr/bulletant/internal/types"
)

type LSMStorage struct {
	dir      string
	opts     LSMOptions
	mem      *memtable
	segments []*segmentReader
	manifest *manifest

	mu           sync.RWMutex
	compactionMu sync.Mutex
}

func NewLSMStorage(opts LSMOptions) (*LSMStorage, error) {
	opts = applyDefaults(opts)
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}

	manifest, err := loadManifest(opts.Dir)
	if err != nil {
		return nil, err
	}
	if err := ensureManifestIDs(manifest); err != nil {
		return nil, err
	}

	sortSegmentsByIDDesc(manifest.Segments)
	segments := make([]*segmentReader, 0, len(manifest.Segments))
	for _, meta := range manifest.Segments {
		reader, err := openSegment(opts.Dir, meta)
		if err != nil {
			closeSegments(segments)
			return nil, err
		}
		segments = append(segments, reader)
	}

	return &LSMStorage{
		dir:      opts.Dir,
		opts:     opts,
		mem:      newMemtable(),
		segments: segments,
		manifest: manifest,
	}, nil
}

func (s *LSMStorage) Get(key types.Key) (types.Value, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getLocked(string(key))
}

func (s *LSMStorage) Put(key types.Key, value types.Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mem.set(string(key), value, false)
	return s.maybeFlushLocked()
}

func (s *LSMStorage) Delete(key types.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.getLocked(string(key)); err != nil {
		return err
	}
	s.mem.set(string(key), nil, true)
	return s.maybeFlushLocked()
}

func (s *LSMStorage) ExecuteTransaction(t *transaction.Transaction) error {
	if t == nil {
		return storage.ErrUnsupported
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	type txnState struct {
		exists    bool
		value     []byte
		tombstone bool
	}
	pending := make(map[string]txnState)
	for _, op := range t.Operations {
		key := string(op.Key)
		state, ok := pending[key]
		if !ok {
			if existing, err := s.getLocked(key); err == nil {
				state = txnState{exists: true, value: existing}
			} else if !errors.Is(err, storage.ErrKeyNotFound) {
				t.Status = transaction.Aborted
				return err
			} else {
				state = txnState{exists: false}
			}
		}
		switch op.Type {
		case types.Put:
			state.value = append([]byte(nil), op.Value...)
			state.exists = true
			state.tombstone = false
			pending[key] = state
		case types.Delete:
			if !state.exists {
				t.Status = transaction.Aborted
				return storage.ErrKeyNotFound
			}
			state.value = nil
			state.exists = false
			state.tombstone = true
			pending[key] = state
		default:
			t.Status = transaction.Aborted
			return storage.ErrUnsupported
		}
	}

	for key, entry := range pending {
		s.mem.set(key, entry.value, entry.tombstone)
	}
	if err := s.maybeFlushLocked(); err != nil {
		t.Status = transaction.Aborted
		return err
	}
	t.Status = transaction.Committed
	return nil
}

func (s *LSMStorage) Scan(req storage.ScanRequest) (storage.ScanResult, error) {
	if err := req.Validate(); err != nil {
		return storage.ScanResult{}, err
	}

	s.mu.RLock()
	memEntries := s.mem.entriesSnapshot()
	segments := append([]*segmentReader(nil), s.segments...)
	s.mu.RUnlock()

	iterators := make([]entryIterator, 0, len(segments)+1)
	iterators = append(iterators, newMemIterator(memEntries, req.IncludeValues, req.MaxValueBytes, string(req.Cursor)))
	for _, seg := range segments {
		iterators = append(iterators, newSegmentIterator(seg, req.IncludeValues, req.MaxValueBytes, string(req.Cursor)))
	}

	entries, nextCursor, err := mergeForScan(
		iterators,
		req.Limit,
		req.IncludeValues,
		req.MaxValueBytes,
		string(req.Prefix),
		string(req.Cursor),
	)
	if err != nil {
		return storage.ScanResult{}, err
	}

	return storage.ScanResult{
		Entries:    entries,
		NextCursor: types.Key(nextCursor),
	}, nil
}

func (s *LSMStorage) AddVector(values []float64, metadata map[string]interface{}) (string, error) {
	return "", storage.ErrUnsupported
}

func (s *LSMStorage) GetVector(id string) (*storage.Vector, error) {
	return nil, storage.ErrUnsupported
}

func (s *LSMStorage) DeleteVector(id string) error {
	return storage.ErrUnsupported
}

func (s *LSMStorage) Compact(opts storage.CompactOptions) (storage.CompactStats, error) {
	return s.compactWithOptions(opts)
}

func (s *LSMStorage) Snapshot(opts storage.SnapshotOptions) (storage.SnapshotStats, error) {
	if opts.Path == "" {
		return storage.SnapshotStats{}, storage.ErrInvalidPath
	}
	if opts.TempPath == "" {
		opts.TempPath = opts.Path + ".tmp"
	}

	s.mu.RLock()
	memEntries := s.mem.entriesSnapshot()
	manifestCopy := copyManifest(s.manifest)
	s.mu.RUnlock()

	if _, err := os.Stat(opts.Path); err == nil {
		return storage.SnapshotStats{}, storage.ErrInvalidPath
	}

	if err := os.MkdirAll(opts.TempPath, 0755); err != nil {
		return storage.SnapshotStats{}, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(opts.TempPath)
		}
	}()

	var totalBytes uint64
	for _, meta := range manifestCopy.Segments {
		src := segmentPath(s.dir, meta.Filename)
		dest := segmentPath(opts.TempPath, meta.Filename)
		bytesCopied, err := copyFileAtomic(src, dest+".tmp", dest)
		if err != nil {
			return storage.SnapshotStats{}, err
		}
		totalBytes += bytesCopied
	}

	if len(memEntries) > 0 {
		id := manifestCopy.NextID
		manifestCopy.NextID++
		filename := segmentFileName(id)
		path := segmentPath(opts.TempPath, filename)
		meta, err := writeSegment(path, memEntries, s.opts.BloomBitsPerKey, nil, context.Background())
		if err != nil {
			return storage.SnapshotStats{}, err
		}
		meta.ID = id
		manifestCopy.Segments = append(manifestCopy.Segments, meta)
		totalBytes += meta.Bytes
	}

	sortSegmentsByIDDesc(manifestCopy.Segments)
	if err := saveManifest(opts.TempPath, manifestCopy); err != nil {
		return storage.SnapshotStats{}, err
	}

	if err := os.Rename(opts.TempPath, opts.Path); err != nil {
		return storage.SnapshotStats{}, err
	}
	cleanup = false

	totalEntries := uint32(0)
	for _, meta := range manifestCopy.Segments {
		totalEntries += meta.Entries
	}

	return storage.SnapshotStats{
		Entries: totalEntries,
		Bytes:   totalBytes,
		Path:    opts.Path,
	}, nil
}

func (s *LSMStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	if err := s.flushMemtableLocked(); err != nil {
		errs = append(errs, err)
	}
	for _, seg := range s.segments {
		if err := seg.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *LSMStorage) getLocked(key string) (types.Value, error) {
	if entry, ok := s.mem.get(key); ok {
		if entry.tombstone {
			return nil, storage.ErrKeyNotFound
		}
		valueCopy := append([]byte(nil), entry.value...)
		return valueCopy, nil
	}

	for _, seg := range s.segments {
		entry, ok, err := seg.lookupEntry(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if entry.tombstone {
			return nil, storage.ErrKeyNotFound
		}
		value, err := seg.readValue(entry)
		if err != nil {
			return nil, err
		}
		return value, nil
	}
	return nil, storage.ErrKeyNotFound
}

func (s *LSMStorage) maybeFlushLocked() error {
	if s.mem.count >= s.opts.MemtableMaxEntries || s.mem.bytes >= s.opts.MemtableMaxBytes {
		return s.flushMemtableLocked()
	}
	return nil
}

func (s *LSMStorage) flushMemtableLocked() error {
	entries := s.mem.entriesSnapshot()
	if len(entries) == 0 {
		return nil
	}

	chunks := splitEntriesBySize(entries, s.opts.SegmentMaxBytes)
	newMetas := make([]segmentMeta, 0, len(chunks))
	newReaders := make([]*segmentReader, 0, len(chunks))

	for _, chunk := range chunks {
		id := s.nextSegmentIDLocked()
		filename := segmentFileName(id)
		path := segmentPath(s.dir, filename)
		meta, err := writeSegment(path, chunk, s.opts.BloomBitsPerKey, nil, context.Background())
		if err != nil {
			closeSegments(newReaders)
			return err
		}
		meta.ID = id
		newMetas = append(newMetas, meta)

		reader, err := openSegment(s.dir, meta)
		if err != nil {
			closeSegments(newReaders)
			return err
		}
		newReaders = append(newReaders, reader)
	}

	s.manifest.Segments = append(s.manifest.Segments, newMetas...)
	sortSegmentsByIDDesc(s.manifest.Segments)
	if err := saveManifest(s.dir, s.manifest); err != nil {
		closeSegments(newReaders)
		for _, meta := range newMetas {
			_ = os.Remove(segmentPath(s.dir, meta.Filename))
		}
		return err
	}

	s.segments = append(s.segments, newReaders...)
	sort.Slice(s.segments, func(i int, j int) bool {
		return s.segments[i].meta.ID > s.segments[j].meta.ID
	})

	s.mem.reset()
	return nil
}

func (s *LSMStorage) nextSegmentIDLocked() uint64 {
	id := s.manifest.NextID
	if id == 0 {
		id = 1
	}
	s.manifest.NextID = id + 1
	return id
}

func (s *LSMStorage) nextSegmentID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextSegmentIDLocked()
}

func (s *LSMStorage) replaceSegments(
	oldSegments []*segmentReader,
	newSegments []segmentMeta,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldIDs := make(map[uint64]struct{}, len(oldSegments))
	for _, seg := range oldSegments {
		oldIDs[seg.meta.ID] = struct{}{}
	}

	updated := make([]segmentMeta, 0, len(s.manifest.Segments)-len(oldSegments)+len(newSegments))
	for _, meta := range s.manifest.Segments {
		if _, ok := oldIDs[meta.ID]; !ok {
			updated = append(updated, meta)
		}
	}
	updated = append(updated, newSegments...)
	s.manifest.Segments = updated
	sortSegmentsByIDDesc(s.manifest.Segments)

	newReaders := make([]*segmentReader, 0, len(s.segments)-len(oldSegments)+len(newSegments))
	for _, seg := range s.segments {
		if _, ok := oldIDs[seg.meta.ID]; ok {
			_ = seg.Close()
			continue
		}
		newReaders = append(newReaders, seg)
	}
	for _, meta := range newSegments {
		reader, err := openSegment(s.dir, meta)
		if err != nil {
			closeSegments(newReaders)
			return err
		}
		newReaders = append(newReaders, reader)
	}
	sort.Slice(newReaders, func(i int, j int) bool {
		return newReaders[i].meta.ID > newReaders[j].meta.ID
	})
	s.segments = newReaders

	if err := saveManifest(s.dir, s.manifest); err != nil {
		return err
	}
	for _, seg := range oldSegments {
		_ = os.Remove(segmentPath(s.dir, seg.meta.Filename))
	}
	return nil
}

func applyDefaults(opts LSMOptions) LSMOptions {
	if opts.MemtableMaxEntries == 0 {
		opts.MemtableMaxEntries = DefaultMemtableMaxEntries
	}
	if opts.MemtableMaxBytes == 0 {
		opts.MemtableMaxBytes = DefaultMemtableMaxBytes
	}
	if opts.SegmentMaxBytes == 0 {
		opts.SegmentMaxBytes = DefaultSegmentMaxBytes
	}
	if opts.BloomBitsPerKey == 0 {
		opts.BloomBitsPerKey = DefaultBloomBitsPerKey
	}
	if opts.CompactionInterval == 0 {
		opts.CompactionInterval = DefaultCompactionInterval
	}
	if opts.CompactionMaxEntries == 0 {
		opts.CompactionMaxEntries = DefaultCompactionMaxEntries
	}
	if opts.CompactionMaxBytes == 0 {
		opts.CompactionMaxBytes = DefaultCompactionMaxBytes
	}
	if opts.CompactionRateLimitBytes == 0 {
		opts.CompactionRateLimitBytes = DefaultCompactionRateLimitBytes
	}
	if opts.CompactionMaxSegments == 0 {
		opts.CompactionMaxSegments = DefaultCompactionMaxSegments
	}
	if opts.CompactionMaxInFlight == 0 {
		opts.CompactionMaxInFlight = DefaultCompactionMaxInFlight
	}
	return opts
}

func ensureManifestIDs(m *manifest) error {
	var maxID uint64
	for _, seg := range m.Segments {
		if seg.ID > maxID {
			maxID = seg.ID
		}
	}
	if m.NextID <= maxID {
		m.NextID = maxID + 1
	}
	return nil
}

func splitEntriesBySize(entries []segmentEntry, maxBytes uint64) [][]segmentEntry {
	if maxBytes == 0 {
		return [][]segmentEntry{entries}
	}
	chunks := make([][]segmentEntry, 0)
	var current []segmentEntry
	var currentSize uint64

	flush := func() {
		if len(current) > 0 {
			chunks = append(chunks, current)
			current = nil
			currentSize = 0
		}
	}

	for _, entry := range entries {
		entrySize := segmentDataEntrySize(entry.key, entry.value) + segmentIndexEntrySize(entry.key)
		if currentSize+entrySize > maxBytes && len(current) > 0 {
			flush()
		}
		current = append(current, entry)
		currentSize += entrySize
	}
	flush()
	return chunks
}

func copyManifest(m *manifest) *manifest {
	if m == nil {
		return &manifest{Version: 1, NextID: 1}
	}
	segments := make([]segmentMeta, len(m.Segments))
	copy(segments, m.Segments)
	return &manifest{
		Version:  m.Version,
		NextID:   m.NextID,
		Segments: segments,
	}
}

func closeSegments(segments []*segmentReader) {
	for _, seg := range segments {
		_ = seg.Close()
	}
}
