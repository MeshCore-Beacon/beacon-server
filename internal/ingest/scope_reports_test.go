package ingest

import (
	"context"
	"strings"
	"testing"
)

func TestScopeReportPresence(t *testing.T) {
	for _, tc := range []struct {
		name, self, field, status, wantSelf string
		wantRows                            int
	}{
		{"missing", "{}", "", "responded", "old", 0},
		{"null", `{"scopes":null}`, `,"scopes":null`, "responded", "old", 0},
		{"empty", `{"scopes":""}`, `,"scopes":""`, "responded", "", 1},
		{"timeout", `{"scopes":"*,on"}`, `,"scopes":""`, "timeout", "*,on", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, base := newTestWorker()
			d := &neighborCaptureDB{stubDB: base, selfScope: "old"}
			w.db = d
			raw := `{"self":` + tc.self + `,"neighbors":[{"pubkey":"` + strings.Repeat("02", 32) + `","status":"` + tc.status + `"` + tc.field + `}]}`
			w.handleNeighbors(context.Background(), "YOW", strings.Repeat("01", 32), []byte(raw))
			if d.selfScope != tc.wantSelf || len(d.scopes) != tc.wantRows {
				t.Fatalf("self=%q rows=%v", d.selfScope, d.scopes)
			}
		})
	}
}
