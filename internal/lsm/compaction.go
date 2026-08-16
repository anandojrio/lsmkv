package lsm

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// This file holds compaction orchestration: size-tiered picking (compactionPlan),
// merge/write (compactReaders), next-manifest construction (manifestAfterCompaction),
// and one full cycle (runCompactionOnce).
//
// Pure merge logic lives in compaction_merge.go (mergeEntries).
//
// Tombstone policy (Phase A / handoff): we NEVER drop tombstones during merge.
// TombstoneGraceSeconds is reserved in Config for a later partner change.
// Keeping deletes on disk is required for Phase B (anti-entropy / replication).

// compactionPlan describes one future compaction operation.
//
// Inputs are the source SSTables to merge (newest-first among the pick).
// OutputID / OutputFile name the replacement SSTable. Building a plan does
// not touch disk or the live manifest.
type compactionPlan struct {
	Inputs     []ManifestTable
	OutputID   uint64
	OutputFile string
}

// newCompactionPlan selects up to K=SizeTieredFanIn live SSTables using the
// deterministic size-tiered picker (see pickSizeTiered).
//
// Returns (nil, nil) when fewer than two tables exist or the picker finds
// nothing — callers treat that as a no-op.
func newCompactionPlan(manifest *Manifest, cfg Config) (*compactionPlan, error) {
	if manifest == nil {
		return nil, fmt.Errorf("create compaction plan: nil manifest: %w", ErrInvalidArgument)
	}
	if len(manifest.Tables) < 2 {
		return nil, nil
	}

	fanIn := cfg.SizeTieredFanIn
	if fanIn < 2 {
		fanIn = 2
	}
	ratio := cfg.SizeTieredSizeRatio
	if ratio < 1.0 {
		ratio = 2.0
	}

	picked, ok := pickSizeTiered(manifest.Tables, fanIn, ratio)
	if !ok || len(picked) < 2 {
		return nil, nil
	}

	outputID := manifest.nextTableID()
	return &compactionPlan{
		Inputs:     picked,
		OutputID:   outputID,
		OutputFile: fmt.Sprintf("%06d.sst", outputID),
	}, nil
}

// pickSizeTiered implements spec §6.4 (deterministic, student-friendly):
//
//  1. Need at least 2 tables.
//  2. Sort by FileSize ascending; tie-break higher ID first (newer first).
//  3. Scan windows of length K: if every size is within sizeRatio of the
//     window's smallest, pick that window.
//  4. Else pick up to K newest tables by ID (still ≥ 2).
//
// Returned slice is ordered newest-first (highest ID first) so mergeEntries
// sees newer layers earlier when used as varargs order.
func pickSizeTiered(tables []ManifestTable, fanIn int, sizeRatio float64) ([]ManifestTable, bool) {
	n := len(tables)
	if n < 2 {
		return nil, false
	}
	if fanIn < 2 {
		fanIn = 2
	}
	if sizeRatio < 1.0 {
		sizeRatio = 1.0
	}
	k := fanIn
	if k > n {
		k = n
	}

	sorted := make([]ManifestTable, n)
	copy(sorted, tables)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].FileSize != sorted[j].FileSize {
			return sorted[i].FileSize < sorted[j].FileSize
		}
		return sorted[i].ID > sorted[j].ID
	})

	for i := 0; i+k <= len(sorted); i++ {
		window := sorted[i : i+k]
		base := window[0].FileSize
		if base <= 0 {
			base = 1
		}
		fit := true
		for _, t := range window {
			if float64(t.FileSize) > float64(base)*sizeRatio {
				fit = false
				break
			}
		}
		if fit {
			return orderTablesNewestFirst(window), true
		}
	}

	// Fallback: up to K newest by ID.
	byNew := make([]ManifestTable, n)
	copy(byNew, tables)
	sort.SliceStable(byNew, func(i, j int) bool {
		return byNew[i].ID > byNew[j].ID
	})
	fallback := byNew
	if len(fallback) > k {
		fallback = fallback[:k]
	}
	if len(fallback) < 2 {
		return nil, false
	}
	return orderTablesNewestFirst(fallback), true
}

func orderTablesNewestFirst(in []ManifestTable) []ManifestTable {
	out := make([]ManifestTable, len(in))
	copy(out, in)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ID > out[j].ID
	})
	return out
}

// compactReaders merges the contents of the supplied SSTables and writes the
// merged result to outputPath.
//
// Reader order should be newest-first when that matches mergeEntries' "first
// set wins on equal seqNo" tie-break (see compaction_merge.go).
//
// Does not update Manifest, publish Version, or remove inputs.
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

// manifestAfterCompaction builds the next manifest after a successful output write.
// Pure: no disk I/O beyond Stat of the output path.
func manifestAfterCompaction(
	current *Manifest,
	plan *compactionPlan,
	outputPath string,
	outputEntries []sstEntry,
) (*Manifest, error) {
	if current == nil {
		return nil, fmt.Errorf("build compacted manifest: nil manifest: %w", ErrInvalidArgument)
	}
	if plan == nil {
		return nil, fmt.Errorf("build compacted manifest: nil plan: %w", ErrInvalidArgument)
	}
	if len(plan.Inputs) == 0 {
		return nil, fmt.Errorf("build compacted manifest: empty inputs: %w", ErrInvalidArgument)
	}
	if len(outputEntries) == 0 {
		return nil, fmt.Errorf("build compacted manifest: empty output: %w", ErrInvalidArgument)
	}

	info, err := os.Stat(outputPath)
	if err != nil {
		return nil, fmt.Errorf("stat compacted output: %w", err)
	}

	inputIDs := make(map[uint64]struct{}, len(plan.Inputs))
	for _, input := range plan.Inputs {
		inputIDs[input.ID] = struct{}{}
	}

	minSeqNo := outputEntries[0].seqNo
	maxSeqNo := outputEntries[0].seqNo
	for _, entry := range outputEntries[1:] {
		if entry.seqNo < minSeqNo {
			minSeqNo = entry.seqNo
		}
		if entry.seqNo > maxSeqNo {
			maxSeqNo = entry.seqNo
		}
	}

	output := ManifestTable{
		ID:       plan.OutputID,
		File:     plan.OutputFile,
		MinKey:   string(outputEntries[0].key),
		MaxKey:   string(outputEntries[len(outputEntries)-1].key),
		MinSeqNo: minSeqNo,
		MaxSeqNo: maxSeqNo,
		FileSize: info.Size(),
	}

	tables := make([]ManifestTable, 0, len(current.Tables)-len(plan.Inputs)+1)
	tables = append(tables, output)
	for _, table := range current.Tables {
		if _, isInput := inputIDs[table.ID]; isInput {
			continue
		}
		tables = append(tables, table)
	}

	return &Manifest{
		Version: current.Version,
		Epoch:   current.Epoch + 1,
		Tables:  tables,
	}, nil
}

// runCompactionOnce performs at most one size-tiered compaction cycle.
//
// Crash-safety order (spec):
//  1. Plan (picker + output id/name)
//  2. Open input readers
//  3. Merge + write output .sst (temp+rename inside writer)
//  4. Build next manifest in memory
//  5. saveManifest (atomic)
//  6. Delete input files only after manifest is durable
//
// Fewer than two tables → no-op, returns current unchanged.
//
// Unit 8: accepts metrics and logger; both may be nil (no-op when nil).
func runCompactionOnce(cfg Config, current *Manifest, metrics *Metrics, logger *slog.Logger) (*Manifest, error) {
	start := time.Now()

	plan, err := newCompactionPlan(current, cfg)
	if err != nil {
		return nil, fmt.Errorf("plan compaction: %w", err)
	}
	if plan == nil {
		return current, nil
	}

	readers := make([]*SSTableReader, 0, len(plan.Inputs))
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	// plan.Inputs is newest-first; open in that order for mergeEntries varargs.
	for _, input := range plan.Inputs {
		reader, err := OpenSSTableReader(filepath.Join(cfg.DataDir, input.File))
		if err != nil {
			return nil, fmt.Errorf("open compaction input %s: %w", input.File, err)
		}
		readers = append(readers, reader)
	}

	outputPath := filepath.Join(cfg.DataDir, plan.OutputFile)

	compactedReader, err := compactReaders(outputPath, cfg, readers...)
	if err != nil {
		return nil, fmt.Errorf("write compacted sstable: %w", err)
	}
	defer func() { _ = compactedReader.Close() }()

	// Unit 9 crash point: SST is on disk but manifest not yet saved.
	// In production this is always a no-op (nil hook).
	runCrashHook("afterSSTRename")

	outputEntries, err := compactedReader.AllEntries()
	if err != nil {
		return nil, fmt.Errorf("read back compacted sstable: %w", err)
	}

	nextManifest, err := manifestAfterCompaction(current, plan, outputPath, outputEntries)
	if err != nil {
		return nil, fmt.Errorf("build next manifest: %w", err)
	}

	if err := saveManifest(cfg, nextManifest); err != nil {
		return nil, fmt.Errorf("save next manifest: %w", err)
	}

	// Delete inputs only after the new manifest is durable.
	for _, input := range plan.Inputs {
		oldPath := filepath.Join(cfg.DataDir, input.File)
		if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("remove old sstable %s: %w", input.File, err)
		}
	}

	// Metrics + log AFTER manifest je durable i input fajlovi obrisani.
	durMs := time.Since(start).Milliseconds()
	if metrics != nil {
		metrics.CompactionsTotal.Add(1)
		metrics.LastCompactDurationMs.Store(durMs)
	}
	if logger != nil {
		logger.Info("compact job",
			"inputs_count", len(plan.Inputs),
			"output_id", plan.OutputID,
			"duration_ms", durMs,
		)
	}

	return nextManifest, nil
}