package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// Node predstavlja jedan čvor u clusteru.
type Node struct {
	ID   string // jedinstveni identifikator, npr. "node1"
	Addr string // mrežna adresa, npr. "localhost:7001"
}

func (n Node) String() string {
	return fmt.Sprintf("%s(%s)", n.ID, n.Addr)
}

// Ring je konzistentni hash ring.
// Svaki fizički čvor dobija VirtualNodes mjesta na ringu
// radi ravnomjerne distribucije ključeva.
type Ring struct {
	mu           sync.RWMutex
	virtualNodes int
	tokens       []token // sortirano po hash vrijednosti
}

type token struct {
	hash uint64
	node Node
}

// New kreira prazan Ring sa datim brojem virtualnih nodova po fizičkom čvoru.
// Preporučena vrijednost: 150.
func New(virtualNodes int) *Ring {
	if virtualNodes < 1 {
		virtualNodes = 1
	}
	return &Ring{virtualNodes: virtualNodes}
}

// Add dodaje čvor na ring. Idempotentno — dodavanje istog čvora dva puta
// ne duplikuje tokene (stari se prvo uklanjaju).
func (r *Ring) Add(n Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Ukloni eventualne stare tokene ovog čvora.
	r.removeNodeLocked(n.ID)

	for i := 0; i < r.virtualNodes; i++ {
		h := hashToken(n.ID, i)
		r.tokens = append(r.tokens, token{hash: h, node: n})
	}
	sort.Slice(r.tokens, func(i, j int) bool {
		return r.tokens[i].hash < r.tokens[j].hash
	})
}

// Remove uklanja čvor sa ringa.
func (r *Ring) Remove(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeNodeLocked(nodeID)
}

func (r *Ring) removeNodeLocked(nodeID string) {
	filtered := r.tokens[:0]
	for _, t := range r.tokens {
		if t.node.ID != nodeID {
			filtered = append(filtered, t)
		}
	}
	r.tokens = filtered
}

// NodesFor vraća listu od n jedinstvenih fizičkih čvorova koji su odgovorni
// za dati ključ. Hoda ring u smjeru kazaljke na satu od hash(key).
// Ako ring ima manje čvorova od n, vraća sve dostupne.
func (r *Ring) NodesFor(key []byte, n int) []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.tokens) == 0 {
		return nil
	}

	h := hashKey(key)
	// Binarnom pretragom nađi prvi token čiji hash >= h.
	idx := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i].hash >= h
	})
	// Ako smo prošli kraj ringa — wrappuj na početak.
	if idx == len(r.tokens) {
		idx = 0
	}

	seen := make(map[string]struct{})
	var result []Node

	for len(result) < n && len(seen) < r.physicalNodeCount() {
		t := r.tokens[idx%len(r.tokens)]
		if _, already := seen[t.node.ID]; !already {
			seen[t.node.ID] = struct{}{}
			result = append(result, t.node)
		}
		idx++
	}
	return result
}

// Nodes vraća sve jedinstvene fizičke čvorove na ringu.
func (r *Ring) Nodes() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]struct{})
	var result []Node
	for _, t := range r.tokens {
		if _, ok := seen[t.node.ID]; !ok {
			seen[t.node.ID] = struct{}{}
			result = append(result, t.node)
		}
	}
	return result
}

// Size vraća broj fizičkih čvorova na ringu.
func (r *Ring) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.physicalNodeCount()
}

// physicalNodeCount broji jedinstvene fizičke čvorove — mora se zvati
// sa već držanim r.mu.RLock.
func (r *Ring) physicalNodeCount() int {
	seen := make(map[string]struct{})
	for _, t := range r.tokens {
		seen[t.node.ID] = struct{}{}
	}
	return len(seen)
}

// hashToken generiše hash za i-ti virtualni nod datog čvora.
func hashToken(nodeID string, i int) uint64 {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s#%d", nodeID, i)))
	return binary.BigEndian.Uint64(h[:8])
}

// hashKey generiše hash za ključ koji tražimo na ringu.
func hashKey(key []byte) uint64 {
	h := sha256.Sum256(key)
	return binary.BigEndian.Uint64(h[:8])
}
