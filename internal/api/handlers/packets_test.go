// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: agpl

package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestGetPacket_InvalidHex(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets/{packetHash}", getPacket(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets/nothex!!", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidPayloadType(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?payloadType=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidRouteType(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?routeType=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidUntil(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?until=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?cursor=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPacketsBackfill_MissingAfterID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets/backfill", listPacketsBackfill(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets/backfill", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPacketsBackfill_InvalidAfterID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets/backfill", listPacketsBackfill(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets/backfill?afterObservationId=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPacketsBackfill_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets/backfill", listPacketsBackfill(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets/backfill?afterObservationId=1&limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
