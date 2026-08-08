package lsm

import (
	"fmt"
	"os"
	"path/filepath"
)

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
