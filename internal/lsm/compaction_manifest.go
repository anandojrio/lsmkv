package lsm

import (
	"fmt"
	"os"
)

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
