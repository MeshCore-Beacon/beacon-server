// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"strings"
	"testing"
)

// The observer's own scope is written only when the report states it.
func TestNeighborReportSelfScope(t *testing.T) {
	for _, tc := range []struct {
		name, self string // the report's "self" member, "" for none
		want       string
	}{
		{"self missing", ``, "old"},
		{"self null", `"self":null,`, "old"},
		{"scopes missing", `"self":{"default_scope":"#bc"},`, "old"},
		{"scopes null", `"self":{"scopes":null},`, "old"},
		{"no regional scope", `"self":{"scopes":"*"},`, "*"},
		{"empty string", `"self":{"scopes":""},`, ""},
		{"scopes listed", `"self":{"scopes":"*,#bc","default_scope":"#bc"},`, "*,#bc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, base := newTestWorker()
			d := &neighborCaptureDB{stubDB: base, selfScope: "old"}
			w.db = d
			w.handleNeighbors(context.Background(), "YOW", strings.Repeat("01", 32), []byte(`{`+tc.self+`"neighbors":[]}`))
			if d.selfScope != tc.want {
				t.Fatalf("stored scope %q, want %q", d.selfScope, tc.want)
			}
		})
	}
}

// A neighbor's scope is written only from an entry that responded; a timeout keeps what was known.
func TestNeighborReportNeighborScope(t *testing.T) {
	for _, tc := range []struct {
		name, entry string // the entry's members after pubkey
		want        []string
	}{
		{"responded", `"status":"responded","scopes":"*,#on"`, []string{"*,#on"}},
		{"responded with none", `"status":"responded","scopes":"*"`, []string{"*"}},
		{"responded without scopes", `"status":"responded"`, []string{""}},
		{"timeout", `"status":"timeout","scopes":""`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, base := newTestWorker()
			d := &neighborCaptureDB{stubDB: base}
			w.db = d
			raw := `{"self":{"scopes":"*"},"neighbors":[{"pubkey":"` + strings.Repeat("02", 32) + `",` + tc.entry + `}]}`
			w.handleNeighbors(context.Background(), "YOW", strings.Repeat("01", 32), []byte(raw))
			if strings.Join(d.scopes, "|") != strings.Join(tc.want, "|") || len(d.scopes) != len(tc.want) {
				t.Fatalf("neighbor scopes %q, want %q", d.scopes, tc.want)
			}
		})
	}
}
