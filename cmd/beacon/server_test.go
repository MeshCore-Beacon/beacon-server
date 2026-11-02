// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestServerTimeouts(t *testing.T) {
	s := newHTTPServer(":0", http.NotFoundHandler())
	if s.ReadHeaderTimeout != 10*time.Second || s.IdleTimeout != 120*time.Second || s.WriteTimeout != 0 || s.ReadTimeout != 0 || s.ErrorLog == nil {
		t.Fatalf("%+v", s)
	}
	if s.Addr != ":0" || s.Handler == nil {
		t.Fatalf("addr %q handler %v", s.Addr, s.Handler)
	}
}

func TestMetricsMux(t *testing.T) {
	h := metricsMux()
	get := func(method, path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		return w
	}
	if w := get(http.MethodGet, "/metrics"); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "go_goroutines") {
		t.Fatalf("GET /metrics: %d %.200s", w.Code, w.Body.String())
	}
	for _, path := range []string{"/", "/api/v1/info", "/api/v1/admin/config", "/metrics/x", "/debug/pprof/"} {
		if w := get(http.MethodGet, path); w.Code != http.StatusNotFound {
			t.Errorf("GET %s: %d, want 404", path, w.Code)
		}
	}
	if w := get(http.MethodPost, "/metrics"); w.Code == http.StatusOK {
		t.Errorf("POST /metrics: %d, want a refusal", w.Code)
	}
}
