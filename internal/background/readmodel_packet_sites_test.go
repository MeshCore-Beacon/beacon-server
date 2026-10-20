// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeSites records every call, in order, across sessions.
type fakeSites struct {
	mu        sync.Mutex
	calls     []string
	state     db.PacketSitesState
	repairs   []int64 // Reconcile results, consumed in order; then 0
	reconErr  []error
	audits    []int64
	verifyBad int // Verify fails this many times, then passes
	verifyErr error
	busy      int // Begin reports a busy lock this many times
	waitBusy  int // Wait returns ErrPreBootWriters this many times
	backfills int // Backfill calls before Complete
}

func (f *fakeSites) log(s string) {
	f.mu.Lock()
	f.calls = append(f.calls, s)
	f.mu.Unlock()
}

func (f *fakeSites) seq() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.calls, ",")
}

func (f *fakeSites) State() db.PacketSitesState { return f.state }
func (f *fakeSites) Init(context.Context) (db.PacketSitesState, error) {
	f.log("init")
	return f.state, nil
}
func (f *fakeSites) Reconcile(context.Context) (int64, error) {
	f.log("reconcile")
	if len(f.reconErr) > 0 {
		err := f.reconErr[0]
		f.reconErr = f.reconErr[1:]
		if err != nil {
			return 0, err
		}
	}
	if len(f.repairs) == 0 {
		return 0, nil
	}
	n := f.repairs[0]
	f.repairs = f.repairs[1:]
	return n, nil
}
func (f *fakeSites) Backfill(_ context.Context, budget time.Duration) (db.PacketSitesState, error) {
	f.log("backfill")
	if budget != packetSitesBackfillStep {
		f.log("bad-budget")
	}
	if f.backfills--; f.backfills <= 0 {
		f.state.Complete = true
	}
	return f.state, nil
}
func (f *fakeSites) Audit(_ context.Context, start func(lo, hi int64) int64) (int64, error) {
	f.log("audit")
	if a := start(10, 20); a < 10 || a > 20 {
		f.log("bad-start")
	}
	if len(f.audits) == 0 {
		return 0, nil
	}
	n := f.audits[0]
	f.audits = f.audits[1:]
	return n, nil
}
func (f *fakeSites) Verify(context.Context) (db.PacketSitesVerification, error) {
	f.log("verify")
	if f.verifyErr != nil {
		return db.PacketSitesVerification{}, f.verifyErr
	}
	if f.verifyBad > 0 {
		f.verifyBad--
		return db.PacketSitesVerification{Wrong: 1}, nil
	}
	now := time.Now()
	f.state.FirstVerifiedAt = &now
	return db.PacketSitesVerification{PagesCompared: 10}, nil
}
func (f *fakeSites) Close(context.Context) { f.log("close") }

func (f *fakeSites) jobs(tr *db.Trust) *PacketSitesJobs {
	return &PacketSitesJobs{
		Begin: func(context.Context) (PacketSitesSession, bool, error) {
			f.log("begin")
			if f.busy > 0 {
				f.busy--
				return nil, false, nil
			}
			return f, true, nil
		},
		Wait: func(context.Context) error {
			f.log("wait")
			if f.waitBusy > 0 {
				f.waitBusy--
				return fmt.Errorf("%w: 1 session(s), pids [7]", db.ErrPreBootWriters)
			}
			return nil
		},
		Trust:     tr,
		Interval:  time.Millisecond,
		busyRetry: time.Millisecond,
	}
}

func TestPacketSitesJobsBootOrder(t *testing.T) {
	ctx := context.Background()
	f := &fakeSites{waitBusy: 2, busy: 1, backfills: 2, repairs: []int64{4}}
	tr := db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)
	before := repairedRows("packet_sites")
	j := f.jobs(tr)
	j.Boot(ctx)
	want := "wait,wait,wait,begin,begin,init,reconcile,backfill,backfill,verify,close"
	if got := f.seq(); got != want {
		t.Fatalf("boot calls %s, want %s", got, want)
	}
	if !tr.Trusted() {
		t.Fatal("trust not earned after a passing verification")
	}
	if got := repairedRows("packet_sites") - before; got != 4 {
		t.Fatalf("repaired counter grew by %v, want 4", got)
	}
	if age := watermarkAge(t, "packet_sites"); age < 0 || age > 5 {
		t.Fatalf("watermark age %v after the boot", age)
	}

	// Verified at an earlier boot: no verification.
	now := time.Now()
	f = &fakeSites{state: db.PacketSitesState{Complete: true, FirstVerifiedAt: &now}}
	tr = db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)
	f.jobs(tr).Boot(ctx)
	if got := f.seq(); got != "wait,begin,init,reconcile,close" || !tr.Trusted() {
		t.Fatalf("boot calls %s, trusted %v", got, tr.Trusted())
	}

	// A failing reconcile: no trust, the boot retries after the interval.
	f = &fakeSites{state: db.PacketSitesState{Complete: true, FirstVerifiedAt: &now}, reconErr: []error{errors.New("boom")}}
	tr = db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)
	f.jobs(tr).Boot(ctx)
	if got := f.seq(); got != "wait,begin,init,reconcile,close,begin,init,reconcile,close" || !tr.Trusted() {
		t.Fatalf("boot calls %s, trusted %v", got, tr.Trusted())
	}

	// A failing or erroring verification: the boot repair is done, trust waits for the task, which retries it.
	for _, f := range []*fakeSites{
		{state: db.PacketSitesState{Complete: true}, verifyBad: 1},
		{state: db.PacketSitesState{Complete: true}, verifyErr: errors.New("verify boom")},
	} {
		tr = db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)
		j = f.jobs(tr)
		j.Boot(ctx)
		if tr.Trusted() || !j.booted.Load() {
			t.Fatalf("trusted %v after a failed verification, booted %v", tr.Trusted(), j.booted.Load())
		}
		f.verifyErr = nil
		if err := j.Task().Run(ctx); err != nil {
			t.Fatal(err)
		}
		if !tr.Trusted() || !strings.HasSuffix(f.seq(), "begin,init,reconcile,verify,close") {
			t.Fatalf("task after a failed verification: %s, trusted %v", f.seq(), tr.Trusted())
		}
	}
}

func TestPacketSitesJobsTaskPolicy(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	f := &fakeSites{state: db.PacketSitesState{Complete: true, FirstVerifiedAt: &now}}
	tr := db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)
	j := f.jobs(tr)
	task := j.Task()
	if task.Name != "readmodel.packet_sites" || task.Interval != j.Interval {
		t.Fatalf("task %q every %s", task.Name, task.Interval)
	}
	if err := task.Run(ctx); err != nil || f.seq() != "" {
		t.Fatalf("a run before the boot: %v, calls %s", err, f.seq())
	}
	j.Boot(ctx)
	f.repairs = []int64{2, 0, 0}
	var results []bool
	for range 3 {
		if err := task.Run(ctx); err != nil {
			t.Fatal(err)
		}
		results = append(results, tr.Trusted())
	}
	if !slices.Equal(results, []bool{false, false, true}) {
		t.Fatalf("trust after runs repairing 2,0,0 = %v", results)
	}

	// A busy lock skips the run and leaves the policy alone.
	f.busy = 1
	calls := f.seq()
	if err := task.Run(ctx); err != nil || f.seq() != calls+",begin" || !tr.Trusted() {
		t.Fatalf("a busy run: %v, called %s", err, strings.TrimPrefix(f.seq(), calls))
	}

	// Failed reconciles for packetSitesStaleRuns intervals drop trust; the next successful run restores it.
	boom := errors.New("boom")
	f.reconErr = []error{boom, boom}
	task.Run(ctx) //nolint:errcheck
	task.Run(ctx) //nolint:errcheck
	if !tr.Trusted() {
		t.Fatal("dropped before packetSitesStaleRuns failures")
	}
	f.reconErr = []error{boom}
	task.Run(ctx) //nolint:errcheck
	if tr.Trusted() {
		t.Fatal("trusted after packetSitesStaleRuns failed runs")
	}
	if err := task.Run(ctx); err != nil || !tr.Trusted() {
		t.Fatalf("the next successful run did not restore trust: %v", err)
	}

	// A state that turned incomplete (a rebuild) drops trust.
	f.state = db.PacketSitesState{}
	f.backfills = 1 << 20
	if err := task.Run(ctx); err != nil || tr.Trusted() {
		t.Fatalf("incomplete state: %v, trusted %v", err, tr.Trusted())
	}
	f.state = db.PacketSitesState{Complete: true, FirstVerifiedAt: &now}

	// The audit: a clean slice changes nothing; a repaired one counts as a repairing run.
	f.repairs = nil
	task.Run(ctx) //nolint:errcheck
	task.Run(ctx) //nolint:errcheck
	if !tr.Trusted() {
		t.Fatal("not restored")
	}
	audit := j.AuditTask()
	if audit.Name != "readmodel.packet_sites.audit" || audit.Interval != PacketSitesAuditInterval {
		t.Fatalf("audit task %q every %s", audit.Name, audit.Interval)
	}
	f.audits = []int64{0, 4}
	before := repairedRows("packet_sites")
	if err := audit.Run(ctx); err != nil || !tr.Trusted() {
		t.Fatalf("clean audit: %v, trusted %v", err, tr.Trusted())
	}
	if err := audit.Run(ctx); err != nil || tr.Trusted() {
		t.Fatalf("repairing audit: %v, trusted %v", err, tr.Trusted())
	}
	if got := repairedRows("packet_sites") - before; got != 4 || strings.Contains(f.seq(), "bad-start") {
		t.Fatalf("audit counter +%v, calls %s", got, f.seq())
	}
}

func TestPacketSitesSweepTask(t *testing.T) {
	ctx := context.Background()
	var sweeps, plans, sizes int
	planOK := true
	var sweepErr error
	j := &PacketSitesJobs{
		Sweep: func(context.Context) (db.SweepResult, error) {
			sweeps++
			return db.SweepResult{SiteLive: 7, TypeLive: 6, ScopeLive: 2}, sweepErr
		},
		SweepPlan: func(context.Context) (bool, string, error) { plans++; return planOK, "Seq Scan on packet_sites", nil },
		Bytes:     func(context.Context) (int64, error) { sizes++; return 4096, nil },
	}
	task := j.SweepTask()
	if task.Name != "packet_sites.sweep" || task.Interval != PacketSitesSweepInterval {
		t.Fatalf("task %q every %s", task.Name, task.Interval)
	}
	rows := testutil.ToFloat64(metrics.JobRows.WithLabelValues("packet_sites.sweep"))
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if sweeps != 2 || plans != 1 || sizes != 1 {
		t.Fatalf("sweeps %d, plan checks %d, size reads %d", sweeps, plans, sizes)
	}
	for idx, want := range map[string]float64{"idx_packet_sites_site": 7, "idx_packet_sites_site_type": 6, "idx_packet_sites_site_scope": 2} {
		if got := testutil.ToFloat64(metrics.PacketSitesFrontLive.WithLabelValues(idx)); got != want {
			t.Errorf("front_live{%s} = %v, want %v", idx, got, want)
		}
	}
	if got := testutil.ToFloat64(metrics.JobRows.WithLabelValues("packet_sites.sweep")) - rows; got != 14 {
		t.Errorf("job rows +%v, want 14", got)
	}
	if testutil.ToFloat64(metrics.PacketSitesSweepPlanOK) != 1 || testutil.ToFloat64(metrics.PacketSitesBytes) != 4096 {
		t.Error("plan or size gauge not set")
	}

	// A bad plan: the gauge reads 0 and the sweep still runs. The check runs again an hour later.
	planOK = false
	j.lastPlan.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	if err := task.Run(ctx); err != nil || sweeps != 3 || plans != 2 || testutil.ToFloat64(metrics.PacketSitesSweepPlanOK) != 0 {
		t.Fatalf("bad plan: %v, sweeps %d, plans %d", err, sweeps, plans)
	}

	// A failed sweep: logged, and the sweep age is not reset.
	metrics.MarkPacketSitesSwept(time.Now().Add(-time.Hour))
	sweepErr = errors.New("boom")
	if err := task.Run(ctx); err == nil {
		t.Fatal("sweep error not returned")
	}
	if age := sweepAge(t); age < 3600 {
		t.Fatalf("sweep age %v after a failed sweep", age)
	}
}

func sweepAge(t *testing.T) float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() == "beacon_packet_sites_sweep_age_seconds" {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	return -1
}

func TestPacketSitesReadOnlyTrust(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	fresh := time.Minute
	ok := db.PacketSitesState{Complete: true, FirstVerifiedAt: &now}
	for _, c := range []struct {
		name string
		st   db.PacketSitesState
		v    db.PacketSitesVerification
		age  time.Duration
		err  error
		want bool
	}{
		{"clean", ok, db.PacketSitesVerification{}, fresh, nil, true},
		{"stale writer", ok, db.PacketSitesVerification{}, 2*PacketSitesInterval + time.Second, nil, false},
		{"never verified", db.PacketSitesState{Complete: true}, db.PacketSitesVerification{}, fresh, nil, false},
		{"incomplete", db.PacketSitesState{FirstVerifiedAt: &now}, db.PacketSitesVerification{}, fresh, nil, false},
		{"missing row", ok, db.PacketSitesVerification{Missing: 1}, fresh, nil, false},
		{"wrong row", ok, db.PacketSitesVerification{Wrong: 1}, fresh, nil, false},
		{"check error", ok, db.PacketSitesVerification{}, fresh, errors.New("boom"), false},
	} {
		tr := db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)
		tr.Earn()
		PacketSitesReadOnlyTrust(ctx, func(context.Context) (db.PacketSitesReadOnlyCheck, error) {
			return db.PacketSitesReadOnlyCheck{Found: true, State: c.st, Verification: c.v, Age: c.age}, c.err
		}, tr)
		if tr.Trusted() != c.want {
			t.Errorf("%s: trusted %v, want %v", c.name, tr.Trusted(), c.want)
		}
	}
	tr := db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)
	PacketSitesReadOnlyTrust(ctx, func(context.Context) (db.PacketSitesReadOnlyCheck, error) {
		return db.PacketSitesReadOnlyCheck{Found: true, State: ok, Age: 2*PacketSitesInterval - 30*time.Second}, nil
	}, tr)
	if exp := tr.Expiry(); !tr.Trusted() || exp.Before(time.Now().Add(25*time.Second)) || exp.After(time.Now().Add(31*time.Second)) {
		t.Fatalf("trusted %v until %v, want about 30 s from now", tr.Trusted(), exp)
	}
	if age := watermarkAge(t, "packet_sites"); age < 9*60 {
		t.Fatalf("watermark age %v, want the writer's", age)
	}
}

// A read-only server whose writer never saved the state still publishes a watermark age, counted from its start, so
// the stale-watermark alert can fire.
func TestPacketSitesReadOnlyWatermarkBeforeState(t *testing.T) {
	metrics.SetReadModelWatermark("packet_sites", time.Now().Add(time.Hour))
	PacketSitesReadOnlyTrust(context.Background(), func(context.Context) (db.PacketSitesReadOnlyCheck, error) {
		return db.PacketSitesReadOnlyCheck{}, nil
	}, db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy))
	if age := watermarkAge(t, "packet_sites"); age < 0 {
		t.Fatalf("watermark age %v with no saved state, want counted from the process start", age)
	}
}

// The audit waits while the saved state is incomplete: not-yet-backfilled slices are not misses.
func TestPacketSitesAuditSkipsIncompleteState(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	f := &fakeSites{state: db.PacketSitesState{Complete: true, FirstVerifiedAt: &now}}
	j := f.jobs(db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy))
	j.Boot(ctx)
	f.state = db.PacketSitesState{}
	f.audits = []int64{4}
	if err := j.AuditTask().Run(ctx); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(f.seq(), "audit") {
		t.Fatalf("audit ran on an incomplete state: %s", f.seq())
	}
}
