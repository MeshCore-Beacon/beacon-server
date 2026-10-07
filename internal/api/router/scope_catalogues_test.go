// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/meshmapper"
)

func TestScopeCataloguesDisabledAndReadOnly(t *testing.T) {
	var importer *meshmapper.Importer
	r := New(nil, nil, nil, Options{ScopeCatalogues: importer})
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(method, "/api/v1/scope-catalogues", nil))
		if method == http.MethodGet && (w.Code != 200 || strings.TrimSpace(w.Body.String()) != "[]" || w.Header().Get("Cache-Control") != "public, max-age=60") {
			t.Fatal(w.Code, w.Body.String())
		}
		if method == http.MethodPost && w.Code != 405 {
			t.Fatal(w.Code)
		}
	}
}
