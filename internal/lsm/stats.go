package lsm

type Stats struct {
	EngineStatus     string
	ActiveSegmentID  int
	BytesWritten     int64
	TotalWALSegments int
	LastSeqNo        uint64
	ActiveEntries    int
	ActiveBytes      int64
	ImmutablesCount  int
	ImmutablesBytes  int64
	SSTCount         int
	SSTTotalBytes    int64
}
