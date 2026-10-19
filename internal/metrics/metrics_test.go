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

func TestRegistryGathersPlanguard(t *testing.T) {
	// A GaugeVec with no series is not gathered: set one first.
	PlanguardFailures.WithLabelValues("routes.example", "custom").Set(1)
	defer PlanguardFailures.Reset()
	PlanguardLastRun.Set(1760000000)
	body := scrape(t)
	for _, want := range []string{
		`beacon_planguard_failures{entry="routes.example",mode="custom"} 1`,
		`beacon_planguard_last_run_timestamp_seconds 1.76e+09`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
}

// With the check off neither metric carries a series, so a staleness alert cannot fire on a server that never
// runs it. Once enabled, the timestamp is exported at 0 before the first run, so a check that never succeeds
// trips the alert. (EnablePlanguard cannot be undone: the off state is checked first.)
func TestPlanguardMetricsOffAndEnabled(t *testing.T) {
	PlanguardFailures.Reset()
	PlanguardLastRun.Set(0)
	if body := scrape(t); strings.Contains(body, "\nbeacon_planguard_failures{") || strings.Contains(body, "\nbeacon_planguard_last_run_timestamp_seconds ") {
		t.Errorf("planguard series with the check off:\n%s", body)
	}
	EnablePlanguard()
	if body := scrape(t); !strings.Contains(body, "\nbeacon_planguard_last_run_timestamp_seconds 0\n") {
		t.Errorf("enabled check: no timestamp series at 0")
	}
}
