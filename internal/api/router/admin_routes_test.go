// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

func TestAdditionalAdminRoutesStayAuthenticated(t *testing.T) {
	for _, key := range []string{"", "fixture-key"} {
		calls := 0
		opts := Options{Auth: config.AuthConfig{APIKey: key}, AdminRoutes: map[string]http.Handler{
			"/feature": http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.WriteHeader(http.StatusNoContent)
			}),
		}}
		handler := New(nil, nil, nil, opts)
		// Registration happens at startup; later edits to the caller's map must
		// not replace the registered handler or create a new unprotected route.
		delete(opts.AdminRoutes, "/feature")
		for _, path := range []string{"/api/v1/admin/feature", "/api/v1/admin/feature/nested"} {
			for _, method := range []string{"GET", "POST", "DELETE"} {
				for _, token := range []string{"", "wrong", "fixture-key"} {
					request := httptest.NewRequest(method, path, nil)
					if token != "" {
						request.Header.Set("Authorization", "Bearer "+token)
					}
					response := httptest.NewRecorder()
					before := calls
					handler.ServeHTTP(response, request)
					want := http.StatusServiceUnavailable
					if key != "" {
						want = http.StatusUnauthorized
						if token == key {
							want = http.StatusNoContent
						}
					}
					if response.Code != want || response.Header().Get("Cache-Control") != "no-store" {
						t.Fatalf("%s %s: status=%d want=%d", method, path, response.Code, want)
					}
					if (calls != before) != (want == http.StatusNoContent) {
						t.Fatal("authentication boundary bypassed")
					}
				}
			}
		}
	}
}
