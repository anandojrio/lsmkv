package transport

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"lsmkv/internal/lsm"
)

// mockStore implementira Store interface za testove — ne dira disk.
type mockStore struct {
	data map[string][]byte
}

func newMockStore() *mockStore {
	return &mockStore{data: make(map[string][]byte)}
}

func (m *mockStore) Put(key, value []byte) error {
	if len(key) == 0 {
		return lsm.ErrInvalidArgument
	}
	m.data[string(key)] = value
	return nil
}

func (m *mockStore) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, lsm.ErrInvalidArgument
	}
	v, ok := m.data[string(key)]
	return v, ok, nil
}

func (m *mockStore) Delete(key []byte) error {
	if len(key) == 0 {
		return lsm.ErrInvalidArgument
	}
	delete(m.data, string(key))
	return nil
}

// --- testovi ---

func TestHandlerPutAndGet(t *testing.T) {
	h := NewHandler(newMockStore(), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// PUT
	req, _ := http.NewRequest(http.MethodPut,
		fmt.Sprintf("%s/kv/ime", srv.URL),
		bytes.NewBufferString("Marko"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("put: want 204, got %d", resp.StatusCode)
	}

	// GET
	resp, err = http.Get(fmt.Sprintf("%s/kv/ime", srv.URL))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get: want 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "Marko" {
		t.Fatalf("get: want Marko, got %q", body)
	}
}

func TestHandlerGetMissingReturns404(t *testing.T) {
	h := NewHandler(newMockStore(), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(fmt.Sprintf("%s/kv/nepostoji", srv.URL))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
}

func TestHandlerDelete(t *testing.T) {
	store := newMockStore()
	store.data["grad"] = []byte("Beograd")

	h := NewHandler(store, nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// DELETE
	req, _ := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/kv/grad", srv.URL), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: want 204, got %d", resp.StatusCode)
	}

	// GET nakon delete mora biti 404
	resp, err = http.Get(fmt.Sprintf("%s/kv/grad", srv.URL))
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("after delete: want 404, got %d", resp.StatusCode)
	}
}

func TestHandlerHealth(t *testing.T) {
	h := NewHandler(newMockStore(), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(fmt.Sprintf("%s/health", srv.URL))
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health: want 200, got %d", resp.StatusCode)
	}
}

func TestClientPutGetDelete(t *testing.T) {
	h := NewHandler(newMockStore(), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Ukloni "http://" jer Client sam dodaje.
	addr := srv.URL[7:]
	c := NewClient(addr, 0)

	if err := c.Put([]byte("key"), []byte("val")); err != nil {
		t.Fatalf("client put: %v", err)
	}

	got, found, err := c.Get([]byte("key"))
	if err != nil || !found {
		t.Fatalf("client get: found=%v err=%v", found, err)
	}
	if string(got) != "val" {
		t.Fatalf("client get: want val, got %q", got)
	}

	if err := c.Delete([]byte("key")); err != nil {
		t.Fatalf("client delete: %v", err)
	}

	_, found, err = c.Get([]byte("key"))
	if err != nil || found {
		t.Fatalf("after delete: found=%v err=%v", found, err)
	}
}

func TestClientHealth(t *testing.T) {
	h := NewHandler(newMockStore(), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	addr := srv.URL[7:]
	c := NewClient(addr, 0)

	if err := c.Health(); err != nil {
		t.Fatalf("health: %v", err)
	}
}
