package ring

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
)

type Node struct {
	ID   string
	Addr string
}

type token struct {
	value uint64
	node  Node
}

type Ring struct {
	tokens       []token
	virtualNodes int
}

func New(nodes []Node, virtualNodes int) (*Ring, error) {
	if virtualNodes <= 0 {
		return nil, fmt.Errorf("virtualNodes must be > 0")
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("at least one node is required")
	}

	seen := make(map[string]struct{}, len(nodes))
	tokens := make([]token, 0, len(nodes)*virtualNodes)

	for _, node := range nodes {
		if strings.TrimSpace(node.ID) == "" {
			return nil, fmt.Errorf("node ID cannot be empty")
		}
		if strings.TrimSpace(node.Addr) == "" {
			return nil, fmt.Errorf("node Addr cannot be empty")
		}
		if _, ok := seen[node.ID]; ok {
			return nil, fmt.Errorf("duplicate node ID: %s", node.ID)
		}
		seen[node.ID] = struct{}{}

		for i := 0; i < virtualNodes; i++ {
			tokenKey := fmt.Sprintf("%s#%d", node.ID, i)
			tokens = append(tokens, token{
				value: hashBytes([]byte(tokenKey)),
				node:  node,
			})
		}
	}

	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].value == tokens[j].value {
			return tokens[i].node.ID < tokens[j].node.ID
		}
		return tokens[i].value < tokens[j].value
	})

	return &Ring{
		tokens:       tokens,
		virtualNodes: virtualNodes,
	}, nil
}

func (r *Ring) Coordinator(key []byte) (Node, bool) {
	if r == nil || len(r.tokens) == 0 {
		return Node{}, false
	}

	pos := hashBytes(key)
	idx := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i].value >= pos
	})
	if idx == len(r.tokens) {
		idx = 0
	}

	return r.tokens[idx].node, true
}

func (r *Ring) PreferenceList(key []byte, n int) []Node {
	if r == nil || len(r.tokens) == 0 || n <= 0 {
		return nil
	}

	if n > r.distinctNodeCount() {
		n = r.distinctNodeCount()
	}

	pos := hashBytes(key)
	start := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i].value >= pos
	})
	if start == len(r.tokens) {
		start = 0
	}

	out := make([]Node, 0, n)
	seen := make(map[string]struct{}, n)

	for step := 0; step < len(r.tokens) && len(out) < n; step++ {
		idx := (start + step) % len(r.tokens)
		node := r.tokens[idx].node
		if _, ok := seen[node.ID]; ok {
			continue
		}
		seen[node.ID] = struct{}{}
		out = append(out, node)
	}

	return out
}

func (r *Ring) distinctNodeCount() int {
	if r == nil {
		return 0
	}
	seen := make(map[string]struct{})
	for _, tok := range r.tokens {
		seen[tok.node.ID] = struct{}{}
	}
	return len(seen)
}

func hashBytes(b []byte) uint64 {
	sum := sha1.Sum(b)
	return binary.BigEndian.Uint64(sum[:8])
}
