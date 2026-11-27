// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

// A request that waited too long for a password slot is a retryable 503, not a failure of its own.
func TestUserErrorBusyIs503WithRetryAfter(t *testing.T) {
	w := httptest.NewRecorder()
	userError(w, users.ErrBusy)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") == "" || errCode(t, w) != "service_unavailable" {
		t.Fatalf("%d %v %s", w.Code, w.Header(), w.Body)
	}
}
