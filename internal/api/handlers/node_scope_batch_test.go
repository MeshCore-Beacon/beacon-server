// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNodeScopeBatchValidation(t *testing.T) {
	for _, body := range []string{`{}`, `{"nodeIds":[]}`, `{"nodeIds":["bad"]}`, `{"nodeIds":["00000000-0000-0000-0000-000000000000"]}`, `{"nodeIds":["11111111-1111-1111-1111-111111111111"],"poll":true}`, `{"nodeIds":["11111111-1111-1111-1111-111111111111"]} {}`} {
		w := httptest.NewRecorder()
		nodeScopeBatch(stubReader{})(w, httptest.NewRequest("POST", "/nodes/scopes", strings.NewReader(body)))
		if w.Code != 400 {
			t.Fatalf("accepted %s: %d", body, w.Code)
		}
	}
	body := `{"nodeIds":[` + strings.Repeat(`"11111111-1111-1111-1111-111111111111",`, 1000) + `"11111111-1111-1111-1111-111111111111"]}`
	w := httptest.NewRecorder()
	nodeScopeBatch(stubReader{})(w, httptest.NewRequest("POST", "/nodes/scopes", strings.NewReader(body)))
	if w.Code != 400 {
		t.Fatal("unbounded batch accepted", w.Code)
	}
}
