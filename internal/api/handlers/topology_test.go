// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package handlers

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTopologyWindowValidation(t *testing.T) {
	for _, window := range []string{"", "15m", "1h", "24h", "72h", "0", "garbage"} {
		w := httptest.NewRecorder()
		RoutesRouter(stubReader{}).ServeHTTP(w, httptest.NewRequest("GET", "/topology?window="+window, nil))
		want := 200
		if window == "72h" || window == "0" || window == "garbage" {
			want = 400
		}
		if w.Code != want {
			t.Fatalf("%s: %d", window, w.Code)
		}
	}
}

func TestTopologyRejectsInvalidRegions(t *testing.T) {
	for _, query := range []string{"iata=TOOLONG", "iatas=" + strings.Join([]string{"YKF", "invalid"}, ",")} {
		w := httptest.NewRecorder()
		RoutesRouter(stubReader{}).ServeHTTP(w, httptest.NewRequest("GET", "/topology?"+query, nil))
		if w.Code != 400 {
			t.Fatalf("%s: %d", query, w.Code)
		}
	}
}
