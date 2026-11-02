// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package metrics

import (
	"io"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
	ReadModelRepaired.WithLabelValues("cmi_test").Add(3)
	ReadModelTrusted.WithLabelValues("cmi_test").Set(1)
	SetReadModelWatermark("cmi_test", time.Now().Add(-90*time.Second))
	PacketSitesFrontLive.WithLabelValues("idx_packet_sites_site").Set(4)
	PacketSitesSweepPlanOK.Set(1)
	PacketSitesBytes.Set(8192)
	MarkPacketSitesSwept(time.Now().Add(-90 * time.Second))
	body := scrape(t)
	var sweepAge float64
	for _, line := range strings.Split(body, "\n") {
		if v, ok := strings.CutPrefix(line, "beacon_packet_sites_sweep_age_seconds "); ok {
			sweepAge, _ = strconv.ParseFloat(v, 64)
		}
	}
	if sweepAge < 90 || sweepAge > 120 {
		t.Errorf("sweep age = %v, want about 90", sweepAge)
	}
	var age float64
	for _, line := range strings.Split(body, "\n") {
		if v, ok := strings.CutPrefix(line, `beacon_readmodel_watermark_age_seconds{model="cmi_test"} `); ok {
			age, _ = strconv.ParseFloat(v, 64)
		}
	}
	if age < 90 || age > 120 {
		t.Errorf("watermark age = %v, want about 90", age)
	}
	for _, want := range []string{
		`beacon_readmodel_repaired_rows_total{model="cmi_test"} 3`, `beacon_readmodel_trusted{model="cmi_test"} 1`,
		`beacon_guard_would_refuse_total{family="other",guard="cap"} 1`,
		`beacon_packet_sites_front_live{index="idx_packet_sites_site"} 4`, `beacon_packet_sites_sweep_plan_ok 1`,
		`beacon_packet_sites_bytes 8192`,
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

// The sequence gauge exports the latest snapshot only: a sequence missing from it disappears.
func TestSequenceCollector(t *testing.T) {
	c := &sequenceCollector{}
	count := func() int { return testutilCollect(t, c) }
	if n := count(); n != 0 {
		t.Fatalf("%d series before any snapshot", n)
	}
	SetSequenceUse([]SequenceUse{{Name: "channels_id_seq", UsedRatio: 0.021}, {Name: "regions_id_seq", UsedRatio: 0, Cycle: true}})
	out := scrape(t)
	for _, want := range []string{`beacon_db_sequence_used_ratio{cycle="false",sequence="channels_id_seq"} 0.021`, `beacon_db_sequence_used_ratio{cycle="true",sequence="regions_id_seq"} 0`} {
		if !strings.Contains(out, want) {
			t.Errorf("scrape lacks %q", want)
		}
	}
	SetSequenceUse([]SequenceUse{{Name: "channels_id_seq", UsedRatio: 0.03}})
	out = scrape(t)
	if !strings.Contains(out, `beacon_db_sequence_used_ratio{cycle="false",sequence="channels_id_seq"} 0.03`) || strings.Contains(out, "regions_id_seq") {
		t.Errorf("after the second snapshot:\n%s", out)
	}
}

func testutilCollect(t *testing.T, c prometheus.Collector) int {
	t.Helper()
	ch := make(chan prometheus.Metric, 16)
	c.Collect(ch)
	close(ch)
	n := 0
	for range ch {
		n++
	}
	return n
}
