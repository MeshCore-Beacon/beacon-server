// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeIndexHealth struct {
	health      [][]db.IndexHealth // one entry per run
	runs        int
	unusedCalls []time.Time
}

func (f *fakeIndexHealth) MeasureIndexHealth(context.Context) ([]db.IndexHealth, error) {
	h := f.health[min(f.runs, len(f.health)-1)]
	f.runs++
	return h, nil
}

func (f *fakeIndexHealth) UnusedIndexes(context.Context) ([]db.UnusedIndex, error) {
	f.unusedCalls = append(f.unusedCalls, time.Now())
	return []db.UnusedIndex{{Name: "ih_test_unused", Bytes: 8192}}, nil
}

// hasLabel reports whether the collector holds a series with index=name.
func hasIndexSeries(t *testing.T, name, metric string) bool {
	t.Helper()
	mfs, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metric {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "index" && l.GetValue() == name {
					return true
				}
			}
		}
	}
	return false
}

func TestIndexHealthTaskPublishes(t *testing.T) {
	f := &fakeIndexHealth{health: [][]db.IndexHealth{
		{
			{Name: "ih_test_a", Bytes: 100, Ratio: 2, HasRatio: true},
			{Name: "ih_test_b", Bytes: 50},
		},
		{{Name: "ih_test_b", Bytes: 60}},
	}}
	task := IndexHealthTask(f, time.Hour)
	if task.Name != "index_health" || task.Interval != time.Hour {
		t.Fatalf("task %q every %s", task.Name, task.Interval)
	}
	bytesBefore := testutil.CollectAndCount(metrics.IndexBytes)
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(metrics.IndexBytes.WithLabelValues("ih_test_a")); got != 100 {
		t.Errorf("bytes{a} = %v, want 100", got)
	}
	if got := testutil.ToFloat64(metrics.IndexBloat.WithLabelValues("ih_test_a")); got != 2 {
		t.Errorf("bloat{a} = %v, want 2", got)
	}
	if hasIndexSeries(t, "ih_test_b", "beacon_index_bloat_ratio") {
		t.Error("bloat series for an index without a ratio")
	}
	if got := testutil.CollectAndCount(metrics.IndexBytes); got != bytesBefore+2 {
		t.Errorf("%d index size series, want %d", got, bytesBefore+2)
	}

	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := testutil.CollectAndCount(metrics.IndexBytes); got != bytesBefore+1 {
		t.Errorf("%d index size series after a drop, want %d", got, bytesBefore+1)
	}
	if hasIndexSeries(t, "ih_test_a", "beacon_index_bytes") || hasIndexSeries(t, "ih_test_a", "beacon_index_bloat_ratio") {
		t.Error("dropped index still exported")
	}
	if got := testutil.ToFloat64(metrics.IndexBytes.WithLabelValues("ih_test_b")); got != 60 {
		t.Errorf("bytes{b} = %v, want 60", got)
	}
	metrics.IndexBytes.DeleteLabelValues("ih_test_b")
}

func TestIndexHealthTaskLogsUnusedWeekly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &fakeIndexHealth{health: [][]db.IndexHealth{{}}}
		task := IndexHealthTask(f, time.Hour)
		start := time.Now()
		for _, at := range []time.Duration{0, time.Hour, 167 * time.Hour, 168 * time.Hour} {
			time.Sleep(start.Add(at).Sub(time.Now()))
			if err := task.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if len(f.unusedCalls) != 2 || f.unusedCalls[0].Sub(start) != 0 || f.unusedCalls[1].Sub(start) != 168*time.Hour {
			var at []time.Duration
			for _, c := range f.unusedCalls {
				at = append(at, c.Sub(start))
			}
			t.Errorf("unused indexes read at %v, want [0s 168h0m0s]", at)
		}
	})
}

func TestFormatUnusedIndexes(t *testing.T) {
	got := formatUnusedIndexes([]db.UnusedIndex{{Name: "a", Bytes: 3 << 20}, {Name: "b", Bytes: 8192}})
	if want := "[a (3.0 MiB), b (8.0 KiB)]"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

type fakePgActivity struct {
	a   metrics.PgActivity
	err error
}

func (f *fakePgActivity) PgActivity(context.Context) (metrics.PgActivity, error) { return f.a, f.err }

func TestPgStatsTaskSetsActivity(t *testing.T) {
	f := &fakePgActivity{a: metrics.PgActivity{WALBytes: 5, CheckpointsTimed: 3, CheckpointsRequested: 1, XIDAge: 42}}
	task := PgStatsTask(f, time.Minute)
	if task.Name != "pg_stats" || task.Interval != time.Minute {
		t.Fatalf("task %q every %s", task.Name, task.Interval)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := testutil.GatherAndCompare(metrics.Registry, strings.NewReader(`
# HELP beacon_pg_wal_bytes_total WAL bytes PostgreSQL has generated.
# TYPE beacon_pg_wal_bytes_total counter
beacon_pg_wal_bytes_total 5
`), "beacon_pg_wal_bytes_total"); err != nil {
		t.Error(err)
	}
	f.err = errors.New("relation \"pg_stat_wal\" does not exist")
	for range 3 {
		if err := task.Run(context.Background()); err != nil {
			t.Errorf("error returned: %v", err)
		}
	}
}
