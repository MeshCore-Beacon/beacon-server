package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestGetNode_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}", getNode(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/not-a-uuid", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeObservations_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/observations", listNodeObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/bad/observations", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeObservations_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/observations", listNodeObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/observations?cursor=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeObservations_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/observations", listNodeObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/observations?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidType(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?type=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?cursor=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidPubkey(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?pubkey=nothex!!", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidSupportsMultibytePaths(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?supportsMultibytePaths=notabool", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidSupportsMultibyteTraces(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?supportsMultibyteTraces=notabool", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
