// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: agpl

package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestGetObserverTelemetry_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/telemetry", getObserverTelemetry(stubReader{}))

	req := httptest.NewRequest(http.MethodGet, "/observers/not-a-uuid/telemetry", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetObserverTelemetry_InvalidRange(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/telemetry", getObserverTelemetry(stubReader{}))

	req := httptest.NewRequest(http.MethodGet, "/observers/00000000-0000-0000-0000-000000000001/telemetry?range=banana", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetObserverTelemetry_InvalidAfterID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/telemetry", getObserverTelemetry(stubReader{}))

	req := httptest.NewRequest(http.MethodGet, "/observers/00000000-0000-0000-0000-000000000001/telemetry?afterId=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetObserverTelemetry_InvalidInterval(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/telemetry", getObserverTelemetry(stubReader{}))

	req := httptest.NewRequest(http.MethodGet, "/observers/00000000-0000-0000-0000-000000000001/telemetry?interval=2h", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
