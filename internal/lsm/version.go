package lsm

type Version struct {
	Epoch uint64
	// Later:
	// Active     *memtable
	// Immutables []*memtable
	// SSTables   []*SSTableReader
}

func newVersionFromManifest(m *Manifest) *Version {
	return &Version{
		Epoch: m.Epoch,
	}
}
