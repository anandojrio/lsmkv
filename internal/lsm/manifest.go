package lsm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type ManifestTable struct {
	ID       uint64 `json:"id"`
	File     string `json:"file"`
	MinKey   string `json:"min_key"`
	MaxKey   string `json:"max_key"`
	MinSeqNo uint64 `json:"min_seq_no"`
	MaxSeqNo uint64 `json:"max_seq_no"`
	FileSize int64  `json:"file_size"`
}

type Manifest struct {
	Version int             `json:"version"`
	Epoch   uint64          `json:"epoch"`
	Tables  []ManifestTable `json:"tables"` // newest first
}

func manifestPath(cfg Config) string {
	return filepath.Join(cfg.DataDir, "manifest.json")
}

func loadManifest(cfg Config) (*Manifest, error) {
	path := manifestPath(cfg)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Manifest{
				Version: 1,
				Epoch:   0,
				Tables:  nil,
			}, nil
		}
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", ErrCorruptionDetected)
	}

	if m.Version == 0 {
		return nil, fmt.Errorf("manifest missing version: %w", ErrCorruptionDetected)
	}

	return &m, nil
}

func saveManifest(cfg Config, m *Manifest) error {
	path := manifestPath(cfg)
	tmp := path + ".tmp"

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}

	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write manifest tmp: %w", err)
	}

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}

	return nil
}

// withNewTable returns a copy of the manifest with t prepended (newest
// first) and the epoch incremented. The original manifest is not mutated.
func (m *Manifest) withNewTable(t ManifestTable) *Manifest {
	tables := make([]ManifestTable, 0, len(m.Tables)+1)
	tables = append(tables, t)
	tables = append(tables, m.Tables...)

	return &Manifest{
		Version: m.Version,
		Epoch:   m.Epoch + 1,
		Tables:  tables,
	}
}

// nextTableID returns the next unused table id, based on the highest id
// currently recorded in the manifest.
func (m *Manifest) nextTableID() uint64 {
	var max uint64
	for _, t := range m.Tables {
		if t.ID > max {
			max = t.ID
		}
	}
	return max + 1
}
