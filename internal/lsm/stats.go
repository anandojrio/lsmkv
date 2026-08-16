package lsm

// Stats is a point-in-time snapshot of store state returned by Store.Stats().
// It combines structural engine state (memtable, WAL, SSTs) with the
// cumulative metrics counters from Metrics.Snapshot().
type Stats struct {
	// Engine lifecycle
	EngineStatus string

	// WAL
	ActiveSegmentID  int
	BytesWritten     int64
	TotalWALSegments int

	// Memtable
	LastSeqNo     uint64
	ActiveEntries int
	ActiveBytes   int64

	// Immutables
	ImmutablesCount int
	ImmutablesBytes int64

	// SSTables
	SSTCount      int
	SSTTotalBytes int64

	// Metrics snapshot (Unit 8)
	Metrics MetricsSnapshot
}
