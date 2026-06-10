// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestGetStatsObservations_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/observations", getStatsObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/observations?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsPayloadBreakdown_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/payload-breakdown", getStatsPayloadBreakdown(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/payload-breakdown?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopNodes_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-nodes", getStatsTopNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-nodes?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopObservers_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-observers", getStatsTopObservers(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-observers?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopObservers_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-observers", getStatsTopObservers(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-observers?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
