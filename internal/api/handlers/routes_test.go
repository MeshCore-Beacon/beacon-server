// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

func TestSearchKnownRoutes_MissingParams(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/routes/search", searchKnownRoutes(stubReader{}))

	tests := []struct {
		name  string
		query string
	}{
		{"missing all", ""},
		{"missing from and to", "?iata=YVR"},
		{"missing to", "?iata=YVR&from=aa"},
		{"missing iata", "?from=aa&to=bb"},
	}
	for _, tt := range tests {
		req := httptest.NewRequest(http.MethodGet, "/routes/search"+tt.query, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d", tt.name, w.Code)
		}
	}
}

func TestSearchCrossIATARoutes_MissingParams(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/routes/cross", searchCrossIATARoutes(stubReader{}))

	tests := []struct {
		name  string
		query string
	}{
		{"missing all", ""},
		{"missing toHash and toIata", "?fromHash=aa&fromIata=YVR"},
		{"missing fromIata", "?fromHash=aa&toHash=bb&toIata=YYJ"},
		{"missing fromHash", "?fromIata=YVR&toHash=bb&toIata=YYJ"},
	}
	for _, tt := range tests {
		req := httptest.NewRequest(http.MethodGet, "/routes/cross"+tt.query, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d", tt.name, w.Code)
		}
	}
}

func TestListKnownRoutes_CursorID(t *testing.T) {
	var gotCursor time.Time
	var gotID int64
	r := chi.NewRouter()
	r.Get("/routes", listKnownRoutes(stubReader{
		listKnownRoutes: func(_ context.Context, _ string, _ int32, cursor time.Time, cursorID int64, _ int32) ([]api.KnownRoute, error) {
			gotCursor, gotID = cursor, cursorID
			return nil, nil
		},
	}))
	for _, tc := range []struct {
		query string
		code  int
		id    int64
	}{
		{"cursor=1700000000000&cursorId=42", http.StatusOK, 42},
		{"cursor=1700000000000", http.StatusOK, 0},
		{"cursor=1700000000000&cursorId=0", http.StatusBadRequest, 0},
		{"cursor=1700000000000&cursorId=x", http.StatusBadRequest, 0},
		{"cursorId=42", http.StatusBadRequest, 0},
	} {
		gotID = 0
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/routes?"+tc.query, nil))
		if w.Code != tc.code {
			t.Errorf("%s: code %d, want %d", tc.query, w.Code, tc.code)
			continue
		}
		if tc.code == http.StatusOK && (gotID != tc.id || gotCursor.UnixMilli() != 1700000000000) {
			t.Errorf("%s: cursor %v id %d", tc.query, gotCursor, gotID)
		}
	}
}

func TestListKnownRoutes_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/routes", listKnownRoutes(stubReader{
		listKnownRoutes: func(_ context.Context, _ string, _ int32, _ time.Time, _ int64, _ int32) ([]api.KnownRoute, error) {
			return []api.KnownRoute{{IATA: "YVR"}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/routes", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestSearchKnownRoutes_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/routes/search", searchKnownRoutes(stubReader{
		searchKnownRoutes: func(_ context.Context, _, _, _ string) ([]api.KnownRoute, error) {
			return []api.KnownRoute{{IATA: "YVR"}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/routes/search?iata=YVR&from=aa&to=bb", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestSearchCrossIATARoutes_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/routes/cross", searchCrossIATARoutes(stubReader{
		searchCrossIATARoutes: func(_ context.Context, _, _, _, _ string) ([]api.CrossIATARoute, error) {
			return []api.CrossIATARoute{}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/routes/cross?fromHash=aa&fromIata=YVR&toHash=bb&toIata=YYJ", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}
