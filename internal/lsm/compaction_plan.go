package lsm

import "fmt"

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
