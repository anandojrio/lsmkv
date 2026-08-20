package node

import "lsmkv/internal/ring"

const DefaultVirtualNodes = 32

func BuildClusterNodes(cfg Config) []ring.Node {
	nodes := make([]ring.Node, 0, 1+len(cfg.SeedNodes))
	nodes = append(nodes, ring.Node{
		ID:   cfg.NodeID,
		Addr: cfg.ListenAddr,
	})

	for _, peer := range cfg.SeedNodes {
		nodes = append(nodes, ring.Node{
			ID:   peer.NodeID,
			Addr: peer.ListenAddr,
		})
	}

	return nodes
}

func BuildRing(cfg Config, virtualNodes int) (*ring.Ring, error) {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}
	return ring.New(BuildClusterNodes(cfg), virtualNodes)
}
