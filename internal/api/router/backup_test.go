// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/backup"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

func TestBackupAuthenticationBeforeExport(t *testing.T) {
	for _, key := range []string{"", "fixture-key"} {
		handler := New(nil, nil, nil, Options{Auth: config.AuthConfig{APIKey: key}, AdminRoutes: map[string]http.Handler{"/backup": handlers.BackupRouter(backup.Options{ConnectionService: "fixture"})}})
		for _, token := range []string{"", "wrong", "fixture-key"} {
			r := httptest.NewRequest("GET", "/api/v1/admin/backup?database=other", nil)
			if token != "" {
				r.Header.Set("Authorization", "Bearer "+token)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			want := 503
			if key != "" {
				want = 401
				if token == key {
					want = 400
				}
			}
			if w.Code != want || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d want=%d", w.Code, want)
			}
		}
	}
}
