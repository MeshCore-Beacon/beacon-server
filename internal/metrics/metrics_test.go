// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func scrape(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	b, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestHandlerServesBeaconMetrics(t *testing.T) {
	GuardWouldRefuse.WithLabelValues("other", "cap").Inc()
	SetPgActivity(PgActivity{WALBytes: 7, CheckpointsTimed: 2, CheckpointsRequested: 1, XIDAge: 9})
	body := scrape(t)
	for _, want := range []string{
		`beacon_guard_would_refuse_total{family="other",guard="cap"} 1`,
		`beacon_pg_wal_bytes_total 7`, `beacon_pg_checkpoints_total{kind="requested"} 1`, `beacon_pg_xid_age 9`,
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
}
