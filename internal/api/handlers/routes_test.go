// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: agpl

package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

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
