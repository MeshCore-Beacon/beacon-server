// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
)

// A nil reader makes any handler that runs panic into a 500, so a 400 proves the
// request was turned away before it reached one.
func TestHugeIATAsListRejectedBeforeHandler(t *testing.T) {
	handler := New(hub.New(), nil, nil, Options{})
	iatas := strings.TrimSuffix(strings.Repeat("YOW,", 250_000), ",")
	for _, path := range []string{"/api/v1/nodes", "/api/v1/packets", "/api/v1/routes/search"} {
		req := httptest.NewRequest(http.MethodGet, path+"?iatas="+iatas, nil)
		rec := httptest.NewRecorder()
		start := time.Now()
		handler.ServeHTTP(rec, req)
		elapsed := time.Since(start)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status %d, want 400; body %.200s", path, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "iatas: at most 1000 values") {
			t.Fatalf("%s: body %.200s", path, rec.Body.String())
		}
		if elapsed > time.Second {
			t.Fatalf("%s: took %v", path, elapsed)
		}
	}
}
