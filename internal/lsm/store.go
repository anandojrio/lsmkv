package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	cfg    Config
	closed bool
	stats  Stats

	mu sync.RWMutex

	wal        *WAL
	mem        *Memtable
	immutables []*Memtable
	seqNo      uint64
	version    *Version
	manifest   *Manifest

	// bg owns flush/compaction workers. It is started in Open and stopped in Close.
	bg *scheduler

	// lastBGError is the most recent background worker failure, if any.
	// It is exposed through Stats.EngineStatus so operators can see silent
	// background problems without crashing the process.
	lastBGError error
}

func Open(cfg Config) (*Store, error) {
	manifest, err := loadManifest(cfg)
	if err != nil {
		return nil, err
	}

	readers := make([]*SSTableReader, 0, len(manifest.Tables))
	for _, t := range manifest.Tables {
		r, err := OpenSSTableReader(filepath.Join(cfg.DataDir, t.File))
		if err != nil {
			for _, opened := range readers {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("open manifest sstable %s: %w", t.File, err)
		}
		readers = append(readers, r)
	}

	version := newVersionFromManifest(manifest, readers)

	wal, err := OpenWAL(cfg)
	if err != nil {
		_ = version.Close()
		return nil, err
	}

	records, err := ReplayWAL(cfg)
	if err != nil {
		_ = wal.Close()
		_ = version.Close()
		return nil, err
	}

	mem := newMemtable()
	var maxSeq uint64

	for _, rec := range records {
		switch rec.Op {
		case WALOpPut:
			mem.Put(rec.Key, rec.Value, rec.SeqNo)
		case WALOpDel:
			mem.Delete(rec.Key, rec.SeqNo)
		}

		if rec.SeqNo > maxSeq {
			maxSeq = rec.SeqNo
		}
	}

	// Preserve the highest sequence number already durable in SSTables so a
	// post-flush reopen does not restart seqNo at 0.
	for _, t := range manifest.Tables {
		if t.MaxSeqNo > maxSeq {
			maxSeq = t.MaxSeqNo
		}
	}

	store := &Store{
		cfg:        cfg,
		wal:        wal,
		mem:        mem,
		immutables: nil,
		seqNo:      maxSeq,
		version:    version,
		manifest:   manifest,
		stats: Stats{
			EngineStatus: "open",
		},
	}

	// Start background workers after the store is fully constructed so the
	// workers never observe a half-initialized Store.
	store.bg = newScheduler(store, cfg.MaxImmutableTables)

	return store, nil
}

func (s *Store) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	s.seqNo++

	rec := WALRecord{Op: WALOpPut, SeqNo: s.seqNo, Key: key, Value: value}
	if err := s.wal.Append(rec); err != nil {
		return err
	}

	s.mem.Put(key, value, s.seqNo)

	return s.rotateIfNeededLocked()
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, ErrInvalidArgument
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, false, ErrStoreClosed
	}

	if value, tombstone, found := s.mem.Get(key); found {
		if tombstone {
			return nil, false, nil
		}
		return value, true, nil
	}

	// Newest immutable first: later rotations are appended, so reverse scan.
	for i := len(s.immutables) - 1; i >= 0; i-- {
		if value, tombstone, found := s.immutables[i].Get(key); found {
			if tombstone {
				return nil, false, nil
			}
			return value, true, nil
		}
	}

	entry, err := s.version.Get(key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	if entry.tombstone {
		return nil, false, nil
	}

	return entry.value, true, nil
}

func (s *Store) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	s.seqNo++

	rec := WALRecord{Op: WALOpDel, SeqNo: s.seqNo, Key: key}
	if err := s.wal.Append(rec); err != nil {
		return err
	}

	s.mem.Delete(key, s.seqNo)

	return s.rotateIfNeededLocked()
}

// rotateIfNeededLocked freezes the active memtable when it is full and asks
// the background scheduler to flush. Unlike the old path, this does NOT
// perform SSTable disk I/O under the write lock.
func (s *Store) rotateIfNeededLocked() error {
	if s.mem.Bytes() < int64(s.cfg.MemtableMaxBytes) {
		return nil
	}

	if s.cfg.MaxImmutableTables > 0 && len(s.immutables) >= s.cfg.MaxImmutableTables {
		return ErrTooManyImmutables
	}

	s.immutables = append(s.immutables, s.mem)
	s.mem = newMemtable()

	// enqueueFlush is non-blocking and only touches a channel, so it is
	// safe while holding s.mu.
	if s.bg != nil {
		s.bg.enqueueFlush()
	}

	return nil
}

// flushPendingImmutables is used by the background flush worker.
// It repeatedly flushes the oldest immutable until none remain.
func (s *Store) flushPendingImmutables() error {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return ErrStoreClosed
		}
		if len(s.immutables) == 0 {
			s.mu.Unlock()
			return nil
		}
		err := s.flushOldestImmutableLocked()
		s.mu.Unlock()
		if err != nil {
			return err
		}
	}
}

func (s *Store) flushOldestImmutableLocked() error {
	if len(s.immutables) == 0 {
		return nil
	}

	imm := s.immutables[0]
	entries := imm.AllEntries()
	if len(entries) == 0 {
		s.immutables = s.immutables[1:]
		return nil
	}

	id := s.manifest.nextTableID()
	filename := fmt.Sprintf("%06d.sst", id)
	path := filepath.Join(s.cfg.DataDir, filename)

	w := NewSSTableWriter(s.cfg)
	for _, e := range entries {
		w.Add(e.key, e.value, e.seqNo, e.tombstone)
	}

	if err := w.Flush(path); err != nil {
		return fmt.Errorf("flush immutable to sstable: %w", err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat flushed sstable: %w", err)
	}

	minKey, maxKey := entries[0].key, entries[len(entries)-1].key
	minSeq, maxSeq := entries[0].seqNo, entries[0].seqNo
	for _, e := range entries[1:] {
		if e.seqNo < minSeq {
			minSeq = e.seqNo
		}
		if e.seqNo > maxSeq {
			maxSeq = e.seqNo
		}
	}

	info := ManifestTable{
		ID:       id,
		File:     filename,
		MinKey:   string(minKey),
		MaxKey:   string(maxKey),
		MinSeqNo: minSeq,
		MaxSeqNo: maxSeq,
		FileSize: fi.Size(),
	}

	newManifest := s.manifest.withNewTable(info)
	if err := saveManifest(s.cfg, newManifest); err != nil {
		return fmt.Errorf("save manifest after flush: %w", err)
	}

	reader, err := OpenSSTableReader(path)
	if err != nil {
		return fmt.Errorf("open freshly flushed sstable: %w", err)
	}

	s.version = s.version.withPublishedFlush(reader, newManifest.Epoch)
	s.manifest = newManifest
	s.immutables = s.immutables[1:]

	// Only reset the WAL when nothing remains in memory that still depends
	// on those WAL records: no immutables left and the active memtable is empty.
	// Otherwise a crash between flush and the next Put could lose unflushed data.
	if len(s.immutables) == 0 && s.mem.Len() == 0 {
		if err := s.wal.Reset(); err != nil {
			return fmt.Errorf("reset wal after flush: %w", err)
		}
	}

	return nil
}

// Compact runs at most one size-tiered compaction cycle: it merges the two
// oldest SSTables in the current manifest into one, publishes the resulting
// manifest, and rebuilds the store's Version so future reads see the merged
// table instead of the two originals.
//
// It is a no-op (returns nil) if fewer than two SSTables currently exist.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	nextManifest, err := runCompactionOnce(s.cfg, s.manifest)
	if err != nil {
		return err
	}

	if nextManifest == s.manifest {
		return nil
	}

	readers := make([]*SSTableReader, 0, len(nextManifest.Tables))
	for _, table := range nextManifest.Tables {
		reader, err := OpenSSTableReader(filepath.Join(s.cfg.DataDir, table.File))
		if err != nil {
			for _, opened := range readers {
				_ = opened.Close()
			}
			return fmt.Errorf("reopen sstables after compaction: %w", err)
		}
		readers = append(readers, reader)
	}

	oldVersion := s.version
	s.version = newVersionFromManifest(nextManifest, readers)
	s.manifest = nextManifest

	if oldVersion != nil {
		_ = oldVersion.Close()
	}

	return nil
}

func (s *Store) ForceFlush() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrStoreClosed
	}

	if err := s.rotateActiveIfNonEmptyLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	// Perform the actual disk work outside the initial lock section by
	// reusing the same path the background worker uses. ForceFlush is
	// synchronous by contract: callers want data on disk before return.
	return s.flushPendingImmutables()
}

func (s *Store) rotateActiveIfNonEmptyLocked() error {
	if s.mem.Len() == 0 {
		return nil
	}

	if s.cfg.MaxImmutableTables > 0 && len(s.immutables) >= s.cfg.MaxImmutableTables {
		return ErrTooManyImmutables
	}

	s.immutables = append(s.immutables, s.mem)
	s.mem = newMemtable()
	return nil
}

func (s *Store) FlushAll() error {
	// FlushAll only drains already-immutable tables. Keep that contract.
	return s.flushPendingImmutables()
}

func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.stats.BytesWritten = s.wal.BytesWritten()
	s.stats.LastSeqNo = s.seqNo
	s.stats.ActiveSegmentID = s.wal.ActiveSegmentID()
	s.stats.TotalWALSegments = s.wal.TotalSegments()
	s.stats.ActiveEntries = s.mem.Len()
	s.stats.ActiveBytes = s.mem.Bytes()
	s.stats.SSTCount = len(s.version.SSTables)

	var sstTotalBytes int64
	for _, t := range s.manifest.Tables {
		sstTotalBytes += t.FileSize
	}
	s.stats.SSTTotalBytes = sstTotalBytes

	s.stats.ImmutablesCount = len(s.immutables)
	s.stats.ImmutablesBytes = 0
	for _, imm := range s.immutables {
		s.stats.ImmutablesBytes += imm.Bytes()
	}

	if s.closed {
		s.stats.EngineStatus = "closed"
	} else if s.lastBGError != nil {
		s.stats.EngineStatus = "degraded: " + s.lastBGError.Error()
	} else {
		s.stats.EngineStatus = "open"
	}

	return s.stats
}

// BGStatus returns a snapshot of background worker state for the CLI.
func (s *Store) BGStatus() BGStatus {
	if s.bg == nil {
		return BGStatus{}
	}
	return s.bg.status()
}

// liveSSTCount is used by the scheduler auto-compact policy.
// Matches Stats().SSTCount: number of live readers in the current Version.
func (s *Store) liveSSTCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.version == nil {
		return 0
	}
	return len(s.version.SSTables)
}

// recordBackgroundError stores the latest background failure for Stats().
func (s *Store) recordBackgroundError(op string, err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastBGError = fmt.Errorf("%s: %w", op, err)
}

// Close is graceful by default (same as CloseGraceful).
func (s *Store) Close() error {
	return s.CloseGraceful()
}

// CloseGraceful stops background workers, drains remaining immutables to
// SSTables, then closes the WAL and Version.
func (s *Store) CloseGraceful() error {
	// Stop workers first so they are not racing Close on WAL/Version.
	if s.bg != nil {
		s.bg.stopGraceful()
	}
	return s.finishClose(true)
}

// CloseFast stops workers quickly and does NOT drain immutables to disk.
// Durable data remains in the WAL and is recovered on the next Open.
func (s *Store) CloseFast() error {
	if s.bg != nil {
		s.bg.stopFast()
	}
	return s.finishClose(false)
}

func (s *Store) finishClose(drainImmutables bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	if drainImmutables {
		for len(s.immutables) > 0 {
			if err := s.flushOldestImmutableLocked(); err != nil {
				closeErr := s.wal.Close()
				_ = s.version.Close()
				s.closed = true
				s.stats.EngineStatus = "closed"
				if closeErr != nil {
					return fmt.Errorf("flush on close: %v; wal close: %w", err, closeErr)
				}
				return fmt.Errorf("flush on close: %w", err)
			}
		}
	}

	if err := s.wal.Close(); err != nil {
		return err
	}

	if err := s.version.Close(); err != nil {
		return err
	}

	s.closed = true
	s.stats.EngineStatus = "closed"
	return nil
}
