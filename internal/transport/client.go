package transport

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client šalje HTTP zahtjeve na jedan čvor.
// Koristi se od strane koordinatora koji treba replicirati write
// na peer čvorove.
type Client struct {
	addr string
	http *http.Client
}

// NewClient kreira Client koji komunicira sa čvorom na datoj adresi.
// timeout se preporučuje 2–5 sekundi za interne pozive.
func NewClient(addr string, timeout time.Duration) *Client {
	return &Client{
		addr: addr,
		http: &http.Client{Timeout: timeout},
	}
}

// Put šalje PUT /kv/{key} sa value kao tijelom zahtjeva.
func (c *Client) Put(key, value []byte) error {
	url := fmt.Sprintf("http://%s/kv/%s", c.addr, key)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(value))
	if err != nil {
		return fmt.Errorf("build put request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("put %s: status %d: %s", key, resp.StatusCode, body)
	}
	return nil
}

// Get šalje GET /kv/{key} i vraća vrijednost.
// Vraća (nil, false, nil) ako ključ ne postoji (404).
func (c *Client) Get(key []byte) ([]byte, bool, error) {
	url := fmt.Sprintf("http://%s/kv/%s", c.addr, key)
	resp, err := c.http.Get(url)
	if err != nil {
		return nil, false, fmt.Errorf("get %s: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, false, fmt.Errorf("get %s: status %d: %s", key, resp.StatusCode, body)
	}
	value, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("read get response: %w", err)
	}
	return value, true, nil
}

// Delete šalje DELETE /kv/{key}.
func (c *Client) Delete(key []byte) error {
	url := fmt.Sprintf("http://%s/kv/%s", c.addr, key)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("build delete request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete %s: status %d: %s", key, resp.StatusCode, body)
	}
	return nil
}

// Health provjerava da li je čvor živ.
// Vraća nil ako je čvor dostupan, error ako nije.
func (c *Client) Health() error {
	url := fmt.Sprintf("http://%s/health", c.addr)
	resp, err := c.http.Get(url)
	if err != nil {
		return fmt.Errorf("health %s: %w", c.addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health %s: status %d", c.addr, resp.StatusCode)
	}
	return nil
}
