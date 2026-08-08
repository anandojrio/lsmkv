package lsm

import (
	"fmt"
)

// compactReaders merges the contents of the supplied SSTables and writes the
// merged result to outputPath.
//
// It does not update a Manifest, publish a Version, or remove input files.
// Those operations are intentionally left to the next step, after this
// write-only compaction primitive has been tested.
//
// The caller must supply at least one reader.
func compactReaders(outputPath string, cfg Config, readers ...*SSTableReader) (*SSTableReader, error) {
	if len(readers) == 0 {
		return nil, fmt.Errorf("compact readers: %w", ErrInvalidArgument)
	}

	entrySets := make([][]sstEntry, 0, len(readers))

	for _, reader := range readers {
		if reader == nil {
			return nil, fmt.Errorf("compact readers: nil reader: %w", ErrInvalidArgument)
		}

		entries, err := reader.AllEntries()
		if err != nil {
			return nil, fmt.Errorf("read entries for compaction: %w", err)
		}

		entrySets = append(entrySets, entries)
	}

	merged := mergeEntries(entrySets...)
	if len(merged) == 0 {
		return nil, fmt.Errorf("compact readers produced no entries: %w", ErrInvalidArgument)
	}

	writer := NewSSTableWriter(cfg)
	for _, entry := range merged {
		writer.Add(entry.key, entry.value, entry.seqNo, entry.tombstone)
	}

	if err := writer.Flush(outputPath); err != nil {
		return nil, fmt.Errorf("write compacted sstable: %w", err)
	}

	reader, err := OpenSSTableReader(outputPath)
	if err != nil {
		return nil, fmt.Errorf("open compacted sstable: %w", err)
	}

	return reader, nil
}
