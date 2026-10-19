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

// fakeCmi records every call, in order, across sessions.
type fakeCmi struct {
	mu        sync.Mutex
	calls     []string
	state     db.CmiState
	repairs   []int64 // Reconcile results, consumed in order; then 0
	reconErr  []error
	verifyBad int // Verify fails this many times, then passes
	busy      int // Begin reports a busy lock this many times
	waitBusy  int // Wait returns ErrPreBootWriters this many times
	backfills int // Backfill calls before Complete
}

func (f *fakeCmi) log(s string) {
	f.mu.Lock()
	f.calls = append(f.calls, s)
	f.mu.Unlock()
}

func (f *fakeCmi) seq() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.calls, ",")
}

func (f *fakeCmi) State() db.CmiState { return f.state }
func (f *fakeCmi) Init(context.Context) (db.CmiState, error) {
	f.log("init")
	return f.state, nil
}
func (f *fakeCmi) Reconcile(context.Context) (int64, error) {
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
func (f *fakeCmi) Backfill(context.Context, time.Duration) (db.CmiState, error) {
	f.log("backfill")
	if f.backfills--; f.backfills <= 0 {
		f.state.Complete = true
	}
	return f.state, nil
}
func (f *fakeCmi) Verify(context.Context) (db.CmiVerification, error) {
	f.log("verify")
	if f.verifyBad > 0 {
		f.verifyBad--
		return db.CmiVerification{Missing: 1}, nil
	}
	now := time.Now()
	f.state.FirstVerifiedAt = &now
	return db.CmiVerification{PagesCompared: 10}, nil
}
func (f *fakeCmi) Close(context.Context) { f.log("close") }

func (f *fakeCmi) jobs(tr *db.Trust) *CmiJobs {
	return &CmiJobs{
		Begin: func(context.Context) (CmiSession, bool, error) {
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

func repairedRows(model string) float64 {
	return testutil.ToFloat64(metrics.ReadModelRepaired.WithLabelValues(model))
}

func TestCmiJobsBootOrder(t *testing.T) {
	ctx := context.Background()
	f := &fakeCmi{waitBusy: 2, busy: 1, backfills: 2, repairs: []int64{4}}
	tr := db.NewTrust("cmi", true, db.CmiTrustPolicy)
	before := repairedRows("cmi")
	j := f.jobs(tr)
	j.Boot(ctx)
	want := "wait,wait,wait,begin,begin,init,reconcile,backfill,backfill,verify,close"
	if got := f.seq(); got != want {
		t.Fatalf("boot calls %s, want %s", got, want)
	}
	if !tr.Trusted() {
		t.Fatal("trust not earned after a passing verification")
	}
	if got := repairedRows("cmi") - before; got != 4 {
		t.Fatalf("repaired counter grew by %v, want 4", got)
	}

	// Verified at an earlier boot: no verification.
	now := time.Now()
	f = &fakeCmi{state: db.CmiState{Complete: true, FirstVerifiedAt: &now}}
	tr = db.NewTrust("cmi", true, db.CmiTrustPolicy)
	f.jobs(tr).Boot(ctx)
	if got := f.seq(); got != "wait,begin,init,reconcile,close" || !tr.Trusted() {
		t.Fatalf("boot calls %s, trusted %v", got, tr.Trusted())
	}

	// A failing reconcile: no trust, the boot retries after the interval.
	f = &fakeCmi{state: db.CmiState{Complete: true, FirstVerifiedAt: &now}, reconErr: []error{errors.New("boom")}}
	tr = db.NewTrust("cmi", true, db.CmiTrustPolicy)
	f.jobs(tr).Boot(ctx)
	if got := f.seq(); got != "wait,begin,init,reconcile,close,begin,init,reconcile,close" || !tr.Trusted() {
		t.Fatalf("boot calls %s, trusted %v", got, tr.Trusted())
	}

	// A failing verification: the boot repair is done, trust waits for the task.
	f = &fakeCmi{state: db.CmiState{Complete: true}, verifyBad: 1}
	tr = db.NewTrust("cmi", true, db.CmiTrustPolicy)
	j = f.jobs(tr)
	j.Boot(ctx)
	if tr.Trusted() || !j.booted.Load() {
		t.Fatalf("trusted %v after a failed verification, booted %v", tr.Trusted(), j.booted.Load())
	}
	if err := j.Task().Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !tr.Trusted() || !strings.HasSuffix(f.seq(), "begin,init,reconcile,verify,close") {
		t.Fatalf("task after a failed verification: %s, trusted %v", f.seq(), tr.Trusted())
	}

	// A stop request ends the boot.
	f = &fakeCmi{waitBusy: 1 << 30}
	tr = db.NewTrust("cmi", true, db.CmiTrustPolicy)
	cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	f.jobs(tr).Boot(cctx)
	if tr.Trusted() {
		t.Fatal("trusted after a cancelled boot")
	}
}

func TestCmiJobsTaskPolicy(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	f := &fakeCmi{state: db.CmiState{Complete: true, FirstVerifiedAt: &now}}
	tr := db.NewTrust("cmi", true, db.CmiTrustPolicy)
	j := f.jobs(tr)
	task := j.Task()
	if task.Name != "readmodel.cmi" || task.Interval != j.Interval {
		t.Fatalf("task %q every %s", task.Name, task.Interval)
	}
	if err := task.Run(ctx); err != nil || f.seq() != "" {
		t.Fatalf("a run before the boot: %v, calls %s", err, f.seq())
	}
	j.Boot(ctx)
	f.repairs = []int64{1, 1, 1, 0, 0}
	results := []bool{}
	for range 5 {
		if err := task.Run(ctx); err != nil {
			t.Fatal(err)
		}
		results = append(results, tr.Trusted())
	}
	if !slices.Equal(results, []bool{true, true, false, false, true}) {
		t.Fatalf("trust after runs repairing 1,1,1,0,0 = %v", results)
	}

	// A busy lock skips the run and leaves the policy counters alone.
	f.repairs = []int64{1, 1}
	task.Run(ctx) //nolint:errcheck
	task.Run(ctx) //nolint:errcheck
	f.busy = 1
	calls := f.seq()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if f.seq() != calls+",begin" {
		t.Fatalf("a busy run called %s", strings.TrimPrefix(f.seq(), calls))
	}
	f.repairs = []int64{1}
	task.Run(ctx) //nolint:errcheck
	if tr.Trusted() {
		t.Fatal("the busy run reset the dirty count")
	}

	// A writer failure drops trust until a run that began after it.
	f.repairs = nil
	task.Run(ctx) //nolint:errcheck
	task.Run(ctx) //nolint:errcheck
	if !tr.Trusted() {
		t.Fatal("not restored by two clean runs")
	}
	tr.Invalidate("failed add")
	if tr.Trusted() {
		t.Fatal("trusted after a writer failure")
	}
	f.repairs = []int64{1}
	if err := task.Run(ctx); err != nil || !tr.Trusted() {
		t.Fatalf("the next run did not restore trust: %v", err)
	}

	// Complete but never verified: each run verifies until it passes, then trust is earned.
	f = &fakeCmi{state: db.CmiState{Complete: true}, verifyBad: 2}
	tr = db.NewTrust("cmi", true, db.CmiTrustPolicy)
	j = f.jobs(tr)
	j.Boot(ctx)
	task = j.Task()
	task.Run(ctx) //nolint:errcheck
	if tr.Trusted() {
		t.Fatal("trusted before a passing verification")
	}
	task.Run(ctx) //nolint:errcheck
	if !tr.Trusted() || strings.Count(f.seq(), "verify") != 3 {
		t.Fatalf("calls %s, trusted %v", f.seq(), tr.Trusted())
	}
}

func TestCmiReadOnlyTrust(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	for _, c := range []struct {
		name string
		st   db.CmiState
		v    db.CmiVerification
		err  error
		want bool
	}{
		{"clean", db.CmiState{Complete: true, FirstVerifiedAt: &now}, db.CmiVerification{}, nil, true},
		{"never verified", db.CmiState{Complete: true}, db.CmiVerification{}, nil, false},
		{"incomplete", db.CmiState{FirstVerifiedAt: &now}, db.CmiVerification{}, nil, false},
		{"missing pair", db.CmiState{Complete: true, FirstVerifiedAt: &now}, db.CmiVerification{Missing: 1}, nil, false},
		{"check error", db.CmiState{Complete: true, FirstVerifiedAt: &now}, db.CmiVerification{}, errors.New("boom"), false},
	} {
		tr := db.NewTrust("cmi", true, db.CmiTrustPolicy)
		CmiReadOnlyTrust(ctx, func(context.Context) (db.CmiState, db.CmiVerification, error) { return c.st, c.v, c.err }, tr)
		if tr.Trusted() != c.want {
			t.Errorf("%s: trusted %v, want %v", c.name, tr.Trusted(), c.want)
		}
	}
}
