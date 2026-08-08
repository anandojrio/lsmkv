package lsm

import (
	"fmt"
	"os"
	"path/filepath"
)

// This file holds the compaction orchestration pieces: selecting which
// SSTables to merge (compactionPlan), performing the merge/write to a new
// SSTable (compactReaders), computing the manifest that should replace the
// current one (manifestAfterCompaction), and running one full compaction
// cycle end-to-end (runCompactionOnce).
//
// The pure merge algorithm (mergeEntries) lives in compaction_merge.go
// because it has no I/O and is independently reusable/testable.

// compactionPlan describes one future compaction operation.
//
// Inputs are the source SSTables to merge. OutputID and OutputFile identify
// the new SSTable that will eventually replace them. Creating a plan does not
// modify the manifest or touch files on disk.
type compactionPlan struct {
	Inputs     []ManifestTable
	OutputID   uint64
	OutputFile string
}

// newCompactionPlan selects the two oldest SSTables in manifest.
//
// Manifest tables are stored newest-first, so the two oldest ones are the last
// two elements. Starting with two inputs keeps the first real compaction easy
// to reason about; later we can expand this into size-tiered fan-in selection.
func newCompactionPlan(manifest *Manifest) (*compactionPlan, error) {
	if manifest == nil {
		return nil, fmt.Errorf("create compaction plan: nil manifest: %w", ErrInvalidArgument)
	}

	if len(manifest.Tables) < 2 {
		return nil, nil
	}

	oldestIndex := len(manifest.Tables) - 1
	secondOldestIndex := len(manifest.Tables) - 2

	outputID := manifest.nextTableID()

	return &compactionPlan{
		Inputs: []ManifestTable{
			manifest.Tables[secondOldestIndex],
			manifest.Tables[oldestIndex],
		},
		OutputID:   outputID,
		OutputFile: fmt.Sprintf("%06d.sst", outputID),
	}, nil
}

// compactReaders merges the contents of the supplied SSTables and writes the
// merged result to outputPath.
//
// It does not update a Manifest, publish a Version, or remove input files.
// Those operations are the responsibility of runCompactionOnce below.
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

// manifestAfterCompaction creates the next manifest snapshot after a successful
// compaction output has been written.
//
// It is a pure function: it does not save manifest.json, publish a Version,
// delete old SSTables, or modify the input manifest.
//
// The output table is inserted first because Manifest.Tables is newest-first.
// Input tables are removed by ID.
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
// Order of operations, matching the spec's crash-safety rules:
//  1. Build a plan (which tables to merge, what the output will be named).
//  2. Open readers for the input tables.
//  3. Merge and write the compacted output as a new .sst file (temp+rename
//     already happens inside SSTableWriter.Flush).
//  4. Build the new manifest snapshot in memory.
//  5. Save the new manifest to disk (atomic temp+rename, same as flush).
//  6. Only after the manifest is safely saved, delete the old input files.
//
// If manifest has fewer than two tables, this is a no-op and returns the
// unchanged manifest.
func runCompactionOnce(cfg Config, current *Manifest) (*Manifest, error) {
	plan, err := newCompactionPlan(current)
	if err != nil {
		return nil, fmt.Errorf("plan compaction: %w", err)
	}
	if plan == nil {
		return current, nil
	}

	readers := make([]*SSTableReader, 0, len(plan.Inputs))
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

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
	defer compactedReader.Close()

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

	// Only delete old input files after the new manifest is durably saved.
	// If the process crashes before this point, the old files are still
	// referenced by nothing (new manifest is saved) but still exist on disk —
	// they are simply orphaned and safe to clean up on a later startup pass.
	for _, input := range plan.Inputs {
		oldPath := filepath.Join(cfg.DataDir, input.File)
		if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("remove old sstable %s: %w", input.File, err)
		}
	}

	return nextManifest, nil
}
