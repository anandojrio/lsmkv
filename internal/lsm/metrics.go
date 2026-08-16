package lsm

import "sync/atomic"

// Metrics holds all engine-level counters. All fields are updated via
// sync/atomic so they are safe to increment from any goroutine without
// holding the store mutex.
//
// Counters are cumulative since store Open(). They are never reset.
// Reading a snapshot is done by Metrics.Snapshot().
type Metrics struct {
	// Write path
	PutsTotal    atomic.Int64
	DeletesTotal atomic.Int64

	// Read path — SSTable layer
	BloomChecksTotal atomic.Int64 // every time Bloom.Has() is called
	BloomSkipsTotal  atomic.Int64 // every time Bloom says "not present" → disk read skipped
	BlockReadsTotal  atomic.Int64 // every data block read from disk

	// Background jobs
	FlushesTotal    atomic.Int64
	CompactionsTotal atomic.Int64

	// Timing (milliseconds, last observed value)
	LastFlushDurationMs   atomic.Int64
	LastCompactDurationMs atomic.Int64
}

// MetricsSnapshot is a plain, copyable snapshot of Metrics counters.
// Use Store.Metrics() to obtain one.
type MetricsSnapshot struct {
	PutsTotal             int64
	DeletesTotal          int64
	BloomChecksTotal      int64
	BloomSkipsTotal       int64
	BlockReadsTotal       int64
	FlushesTotal          int64
	CompactionsTotal      int64
	LastFlushDurationMs   int64
	LastCompactDurationMs int64
}

// Snapshot returns a consistent point-in-time copy of all counters.
// Individual fields are each atomic loads; the snapshot is not
// taken under a single lock, so values are "approximately consistent".
// This is intentional — metrics are advisory, not transactional.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		PutsTotal:             m.PutsTotal.Load(),
		DeletesTotal:          m.DeletesTotal.Load(),
		BloomChecksTotal:      m.BloomChecksTotal.Load(),
		BloomSkipsTotal:       m.BloomSkipsTotal.Load(),
		BlockReadsTotal:       m.BlockReadsTotal.Load(),
		FlushesTotal:          m.FlushesTotal.Load(),
		CompactionsTotal:      m.CompactionsTotal.Load(),
		LastFlushDurationMs:   m.LastFlushDurationMs.Load(),
		LastCompactDurationMs: m.LastCompactDurationMs.Load(),
	}
}