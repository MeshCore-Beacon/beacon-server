// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package handlers

import (
	"net/http/httptest"
	"testing"
)

func TestNodeActivityRejectsSubhourWindows(t *testing.T) {
	for _, window := range []string{"1ns", "1m", "59m59s"} {
		w := httptest.NewRecorder()
		NodesRouter(&stubReader{}, false).ServeHTTP(w, httptest.NewRequest("GET", "/00000000-0000-0000-0000-000000000001/activity?range="+window, nil))
		if w.Code != 400 {
			t.Fatalf("range %s: HTTP %d, want 400", window, w.Code)
		}
	}
}
