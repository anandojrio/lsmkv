package ring

import (
	"fmt"
	"testing"
)

func makeNodes(n int) []Node {
	nodes := make([]Node, n)
	for i := range nodes {
		nodes[i] = Node{
			ID:   fmt.Sprintf("node%d", i+1),
			Addr: fmt.Sprintf("localhost:%d", 7001+i),
		}
	}
	return nodes
}

// Ring sa 3 čvora mora uvijek vratiti tačno 3 jedinstvena čvora za bilo koji ključ.
func TestNodesForReturnsNUniqueNodes(t *testing.T) {
	r := New(150)
	for _, n := range makeNodes(3) {
		r.Add(n)
	}

	keys := [][]byte{
		[]byte("alpha"),
		[]byte("bravo"),
		[]byte("charlie"),
		[]byte("delta"),
		[]byte("echo"),
	}

	for _, key := range keys {
		nodes := r.NodesFor(key, 3)
		if len(nodes) != 3 {
			t.Fatalf("key=%q: want 3 nodes, got %d", key, len(nodes))
		}
		// Provjeri jedinstvenost.
		seen := map[string]struct{}{}
		for _, nd := range nodes {
			if _, dup := seen[nd.ID]; dup {
				t.Fatalf("key=%q: duplicate node %s", key, nd.ID)
			}
			seen[nd.ID] = struct{}{}
		}
	}
}

// Isti ključ uvijek mora ići na iste čvorove (determinizam).
func TestNodesForIsDeterministic(t *testing.T) {
	r := New(150)
	for _, n := range makeNodes(3) {
		r.Add(n)
	}

	key := []byte("consistency-check")
	first := r.NodesFor(key, 3)

	for i := 0; i < 10; i++ {
		got := r.NodesFor(key, 3)
		for j := range got {
			if got[j].ID != first[j].ID {
				t.Fatalf("non-deterministic: iter %d pos %d: want %s got %s",
					i, j, first[j].ID, got[j].ID)
			}
		}
	}
}

// Ako tražimo više čvorova nego ih ima, vraćamo sve dostupne.
func TestNodesForCapsAtRingSize(t *testing.T) {
	r := New(150)
	for _, n := range makeNodes(2) {
		r.Add(n)
	}

	nodes := r.NodesFor([]byte("key"), 5)
	if len(nodes) != 2 {
		t.Fatalf("want 2 (ring size), got %d", len(nodes))
	}
}

// Prazan ring mora vratiti nil bez panike.
func TestNodesForEmptyRingReturnsNil(t *testing.T) {
	r := New(150)
	nodes := r.NodesFor([]byte("key"), 3)
	if nodes != nil {
		t.Fatalf("want nil, got %v", nodes)
	}
}

// Remove mora eliminisati čvor — ključ koji je išao na njega sada ide na drugi.
func TestRemoveRedistributesKeys(t *testing.T) {
	r := New(150)
	nodes := makeNodes(3)
	for _, n := range nodes {
		r.Add(n)
	}

	key := []byte("test-remove")
	before := r.NodesFor(key, 1)
	if len(before) == 0 {
		t.Fatal("expected at least one node before remove")
	}

	// Ukloni primarni čvor za ovaj ključ.
	r.Remove(before[0].ID)

	after := r.NodesFor(key, 1)
	if len(after) == 0 {
		t.Fatal("expected at least one node after remove")
	}
	if after[0].ID == before[0].ID {
		t.Fatalf("removed node %s still primary after remove", before[0].ID)
	}
}

// Add istog čvora dva puta ne smije duplikovati tokene.
func TestAddIdempotent(t *testing.T) {
	r := New(10)
	n := Node{ID: "node1", Addr: "localhost:7001"}
	r.Add(n)
	r.Add(n) // drugi put

	if r.Size() != 1 {
		t.Fatalf("want 1 physical node, got %d", r.Size())
	}
}

// Size vraća tačan broj fizičkih čvorova.
func TestSize(t *testing.T) {
	r := New(150)
	if r.Size() != 0 {
		t.Fatal("empty ring size must be 0")
	}
	for i, n := range makeNodes(3) {
		r.Add(n)
		if r.Size() != i+1 {
			t.Fatalf("after adding %d nodes: want size %d, got %d", i+1, i+1, r.Size())
		}
	}
}

// Ravnomjerna distribucija: ni jedan čvor ne smije dobiti više od 2x
// više ključeva od prosjeka (sa 150 virtualnih nodova).
func TestDistributionIsRoughlyUniform(t *testing.T) {
	r := New(150)
	nodes := makeNodes(3)
	for _, n := range nodes {
		r.Add(n)
	}

	counts := make(map[string]int)
	total := 10_000
	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		primary := r.NodesFor(key, 1)
		counts[primary[0].ID]++
	}

	avg := total / len(nodes)
	for _, n := range nodes {
		c := counts[n.ID]
		if c < avg/2 || c > avg*2 {
			t.Errorf("node %s: count %d too far from avg %d", n.ID, c, avg)
		}
	}
}