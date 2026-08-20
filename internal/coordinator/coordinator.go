package coordinator

import (
	"fmt"

	"lsmkv/internal/ring"
)

const DefaultReplicationFactor = 3

type Coordinator struct {
	localNodeID string
	replicas    int
	ring        *ring.Ring
}

func New(localNodeID string, r *ring.Ring, replicas int) (*Coordinator, error) {
	if localNodeID == "" {
		return nil, fmt.Errorf("localNodeID cannot be empty")
	}
	if r == nil {
		return nil, fmt.Errorf("ring cannot be nil")
	}
	if replicas <= 0 {
		replicas = DefaultReplicationFactor
	}

	return &Coordinator{
		localNodeID: localNodeID,
		replicas:    replicas,
		ring:        r,
	}, nil
}

func (c *Coordinator) CoordinatorNode(key []byte) (ring.Node, bool) {
	if c == nil || c.ring == nil {
		return ring.Node{}, false
	}
	return c.ring.Coordinator(key)
}

func (c *Coordinator) PreferenceList(key []byte) []ring.Node {
	if c == nil || c.ring == nil {
		return nil
	}
	return c.ring.PreferenceList(key, c.replicas)
}

func (c *Coordinator) IsLocalReplica(key []byte) bool {
	_, ok := c.LocalIndex(key)
	return ok
}

func (c *Coordinator) LocalIndex(key []byte) (int, bool) {
	if c == nil || c.ring == nil {
		return -1, false
	}

	prefs := c.PreferenceList(key)
	for i, n := range prefs {
		if n.ID == c.localNodeID {
			return i, true
		}
	}
	return -1, false
}
