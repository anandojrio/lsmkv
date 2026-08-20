package node

import (
	"fmt"

	"lsmkv/internal/coordinator"
	"lsmkv/internal/ring"
)

type Runtime struct {
	Config            Config
	ClusterNodes      []ring.Node
	Ring              *ring.Ring
	Coordinator       *coordinator.Coordinator
	ReplicationFactor int
	WriteQuorum       int
	ReadQuorum        int
}

func NewRuntime(cfg Config, virtualNodes int) (*Runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}

	clusterNodes := BuildClusterNodes(cfg)

	r, err := BuildRing(cfg, virtualNodes)
	if err != nil {
		return nil, fmt.Errorf("build ring: %w", err)
	}

	c, err := coordinator.New(cfg.NodeID, r, cfg.ReplicationFactor)
	if err != nil {
		return nil, fmt.Errorf("build coordinator: %w", err)
	}

	return &Runtime{
		Config:            cfg,
		ClusterNodes:      clusterNodes,
		Ring:              r,
		Coordinator:       c,
		ReplicationFactor: cfg.ReplicationFactor,
		WriteQuorum:       cfg.WriteQuorum,
		ReadQuorum:        cfg.ReadQuorum,
	}, nil
}
