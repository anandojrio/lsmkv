package transport

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"lsmkv/internal/lsm"
)

// Store je interface koji transport koristi — odgovara lsm.Store API-ju.
// Definišemo ga ovdje da testovi mogu ubaciti mock bez pravog Store-a.
type Store interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	Delete(key []byte) error
}

// Handler drži HTTP mux i referencu na lokalni Store.
type Handler struct {
	mux    *http.ServeMux
	store  Store
	logger *slog.Logger
}

// NewHandler kreira Handler i registruje sve rute.
func NewHandler(store Store, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	h := &Handler{
		mux:    http.NewServeMux(),
		store:  store,
		logger: logger,
	}
	h.mux.HandleFunc("PUT /kv/{key}", h.handlePut)
	h.mux.HandleFunc("GET /kv/{key}", h.handleGet)
	h.mux.HandleFunc("DELETE /kv/{key}", h.handleDelete)
	h.mux.HandleFunc("GET /health", h.handleHealth)
	return h
}

// ServeHTTP implementira http.Handler — Handler se može direktno predati
// http.Server-u.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// -------------------------------------------------------------------
// PUT /kv/{key}
// Body: raw bytes vrijednosti
// Response 204 No Content on success
// -------------------------------------------------------------------

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.PathValue("key"))

	value, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	defer r.Body.Close()

	if err := h.store.Put(key, value); err != nil {
		h.logger.Error("put failed", "key", string(key), "err", err)
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// -------------------------------------------------------------------
// GET /kv/{key}
// Response 200 + raw bytes on hit
// Response 404 on miss
// -------------------------------------------------------------------

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.PathValue("key"))

	value, found, err := h.store.Get(key)
	if err != nil {
		h.logger.Error("get failed", "key", string(key), "err", err)
		writeStoreError(w, err)
		return
	}
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(value)
}

// -------------------------------------------------------------------
// DELETE /kv/{key}
// Response 204 No Content on success
// -------------------------------------------------------------------

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.PathValue("key"))

	if err := h.store.Delete(key); err != nil {
		h.logger.Error("delete failed", "key", string(key), "err", err)
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// -------------------------------------------------------------------
// GET /health
// Response 200 + JSON {"status":"ok"}
// -------------------------------------------------------------------

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// -------------------------------------------------------------------
// helpers
// -------------------------------------------------------------------

type errorResponse struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: msg})
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, lsm.ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, lsm.ErrStoreClosed):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, lsm.ErrWriteStall):
		writeError(w, http.StatusTooManyRequests, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}
