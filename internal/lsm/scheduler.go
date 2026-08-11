package lsm

import (
	"sync"
	"sync/atomic"
	"time"
)

type jobKind int

const (
	jobFlush jobKind = iota
	jobCompact
)

type job struct {
	kind jobKind
}

// BGStatus is a snapshot of background worker state for CLI/stats.
type BGStatus struct {
	FlushRunning      bool
	CompactRunning    bool
	FlushQueueLen     int
	CompactQueueLen   int
	FlushJobsTotal    uint64
	CompactJobsTotal  uint64
	LastFlushMs       int64
	LastCompactMs     int64
	LastError         string
	CompactionTrigger int
}

type scheduler struct {
	store *Store

	flushQueue   chan job
	compactQueue chan job

	wg     sync.WaitGroup
	stopCh chan struct{}
	once   sync.Once

	flushRunning   atomic.Bool
	compactRunning atomic.Bool
	flushJobs      atomic.Uint64
	compactJobs    atomic.Uint64
	lastFlushMs    atomic.Int64
	lastCompactMs  atomic.Int64
}

func newScheduler(store *Store, queueDepth int) *scheduler {
	if queueDepth <= 0 {
		queueDepth = 4
	}

	s := &scheduler{
		store:        store,
		flushQueue:   make(chan job, queueDepth),
		compactQueue: make(chan job, queueDepth),
		stopCh:       make(chan struct{}),
	}

	s.wg.Add(2)
	go s.runFlushWorker()
	go s.runCompactWorker()
	return s
}

func (s *scheduler) runFlushWorker() {
	defer s.wg.Done()

	for {
		select {
		case <-s.flushQueue:
			s.flushRunning.Store(true)
			start := time.Now()
			err := s.store.flushPendingImmutables()
			s.lastFlushMs.Store(time.Since(start).Milliseconds())
			s.flushJobs.Add(1)
			s.flushRunning.Store(false)

			if err != nil {
				s.store.recordBackgroundError("flush", err)
				continue
			}
			s.maybeEnqueueCompact()

		case <-s.stopCh:
			return
		}
	}
}

func (s *scheduler) runCompactWorker() {
	defer s.wg.Done()

	for {
		select {
		case <-s.compactQueue:
			s.compactRunning.Store(true)
			start := time.Now()
			err := s.store.Compact()
			s.lastCompactMs.Store(time.Since(start).Milliseconds())
			s.compactJobs.Add(1)
			s.compactRunning.Store(false)

			if err != nil {
				s.store.recordBackgroundError("compact", err)
			}

		case <-s.stopCh:
			return
		}
	}
}

func (s *scheduler) enqueueFlush() {
	select {
	case s.flushQueue <- job{kind: jobFlush}:
	default:
		// Queue full: next successful rotation/enqueue or ForceFlush still drains.
	}
}

func (s *scheduler) enqueueCompact() {
	select {
	case s.compactQueue <- job{kind: jobCompact}:
	default:
	}
}

// maybeEnqueueCompact schedules compaction only when live SST count
// meets the configured trigger. trigger <= 0 disables auto-compact.
func (s *scheduler) maybeEnqueueCompact() {
	if s.store == nil {
		return
	}
	trigger := s.store.cfg.L0CompactionTrigger
	if trigger <= 0 {
		return
	}
	if s.store.liveSSTCount() >= trigger {
		s.enqueueCompact()
	}
}

func (s *scheduler) status() BGStatus {
	lastErr := ""
	trigger := 0
	if s.store != nil {
		trigger = s.store.cfg.L0CompactionTrigger
		s.store.mu.RLock()
		if s.store.lastBGError != nil {
			lastErr = s.store.lastBGError.Error()
		}
		s.store.mu.RUnlock()
	}

	return BGStatus{
		FlushRunning:      s.flushRunning.Load(),
		CompactRunning:    s.compactRunning.Load(),
		FlushQueueLen:     len(s.flushQueue),
		CompactQueueLen:   len(s.compactQueue),
		FlushJobsTotal:    s.flushJobs.Load(),
		CompactJobsTotal:  s.compactJobs.Load(),
		LastFlushMs:       s.lastFlushMs.Load(),
		LastCompactMs:     s.lastCompactMs.Load(),
		LastError:         lastErr,
		CompactionTrigger: trigger,
	}
}

func (s *scheduler) stopGraceful() {
	s.once.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
}

func (s *scheduler) stopFast() {
	s.once.Do(func() {
		close(s.stopCh)
	})
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
	}
}
