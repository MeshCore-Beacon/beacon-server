// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

func TestAdminAuthBoundary(t *testing.T) {
	for _, key := range []string{"", "synthetic-test-key"} {
		handler := New(nil, nil, nil, 5, config.CORSConfig{}, config.ServerConfig{}, config.AuthConfig{APIKey: key})
		for _, path := range []string{"/api/v1/admin", "/api/v1/admin/", "/api/v1/admin/config", "/api/v1/admin//config"} {
			for _, method := range []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"} {
				for _, token := range []string{"", "wrong", "synthetic-test-key"} {
					req := httptest.NewRequest(method, path, nil)
					if token != "" {
						req.Header.Set("Authorization", "Bearer "+token)
					}
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, req)
					want := http.StatusServiceUnavailable
					if key != "" {
						want = 401
						if token == key {
							want = 404
						}
					}
					if w.Code != want {
						t.Fatalf("%s %s status=%d want=%d", method, path, w.Code, want)
					}
				}
			}
		}
		for path, want := range map[string]int{"/api/v1/brokers": 200, "/api/v1/administrator": 404, "/swagger": 301} {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
			if w.Code != want {
				t.Fatalf("public %s status=%d want=%d", path, w.Code, want)
			}
		}
		req := httptest.NewRequest("OPTIONS", "/api/v1/admin/config", nil)
		req.Header.Set("Origin", "https://example.test")
		req.Header.Set("Access-Control-Request-Method", "GET")
		req.Header.Set("Access-Control-Request-Headers", "Authorization")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != 200 || w.Header().Get("Access-Control-Allow-Origin") == "" {
			t.Fatal("CORS preflight blocked")
		}
	}
}
