// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func pgReport(results ...planguard.Result) *planguard.Report {
	return &planguard.Report{Tier: "prod", StartedAt: time.Now().UTC(), Results: results}
}

func TestPlanguardTaskRunsDaily(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var at []time.Duration
		pg := NewPlanguard(func(context.Context) (*planguard.Report, error) {
			at = append(at, time.Since(start))
			return pgReport(), nil
		})
		task := pg.Task(24 * time.Hour)
		if task.Name != "planguard" || task.Interval != time.Hour {
			t.Fatalf("task = %s every %v", task.Name, task.Interval)
		}
		if _, ok := pg.Last(); ok {
			t.Fatal("Last before the first run")
		}
		for h := 1; h <= 49; h++ {
			time.Sleep(start.Add(time.Duration(h) * time.Hour).Sub(time.Now()))
			if err := task.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		want := []time.Duration{time.Hour, 25 * time.Hour, 49 * time.Hour}
		if len(at) != len(want) || at[0] != want[0] || at[1] != want[1] || at[2] != want[2] {
			t.Fatalf("ran at %v, want %v", at, want)
		}
	})
}

func TestPlanguardSetsMetric(t *testing.T) {
	first := pgReport(
		planguard.Result{ID: "a", Mode: "custom", Verdict: planguard.VerdictFalsePositive},
		planguard.Result{ID: "b", Mode: "generic", Verdict: planguard.VerdictMissed},
		planguard.Result{ID: "c", Mode: "custom", Verdict: planguard.VerdictRetiredIndex},
		planguard.Result{ID: "d", Mode: "custom", Verdict: planguard.VerdictOK},
	)
	clean := pgReport(planguard.Result{ID: "d", Mode: "custom", Verdict: planguard.VerdictOK})
	errored := pgReport(planguard.Result{ID: "e", Mode: "generic", Verdict: planguard.VerdictError})
	failure := errors.New("no connection")
	var next *planguard.Report
	var nextErr error
	pg := NewPlanguard(func(context.Context) (*planguard.Report, error) { return next, nextErr })
	task := pg.Task(24 * time.Hour)
	metrics.PlanguardFailures.Reset()
	metrics.PlanguardLastRun.Set(0)

	next = first
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v := testutil.ToFloat64(metrics.PlanguardFailures.WithLabelValues("a", "custom")); v != 1 {
		t.Errorf("(a, custom) = %v", v)
	}
	if v := testutil.ToFloat64(metrics.PlanguardFailures.WithLabelValues("b", "generic")); v != 1 {
		t.Errorf("(b, generic) = %v", v)
	}
	if n := testutil.CollectAndCount(metrics.PlanguardFailures); n != 2 {
		t.Errorf("series = %d, want 2", n)
	}
	if v := testutil.ToFloat64(metrics.PlanguardLastRun); v <= 0 {
		t.Errorf("last run = %v", v)
	}

	// A run that fails keeps the previous report and series; the task returns the error.
	pg.lastAt = time.Time{}
	next, nextErr = nil, failure
	if err := task.Run(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("task error = %v, want %v", err, failure)
	}
	if got, ok := pg.Last(); !ok || got != first {
		t.Errorf("Last after a failed run = %v, %v", got, ok)
	}
	if n := testutil.CollectAndCount(metrics.PlanguardFailures); n != 2 {
		t.Errorf("series after a failed run = %d, want 2", n)
	}

	pg.lastAt = time.Time{}
	next, nextErr = clean, nil
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := testutil.CollectAndCount(metrics.PlanguardFailures); n != 0 {
		t.Errorf("series after a clean run = %d, want 0", n)
	}

	// ERROR counts as a failure.
	pg.lastAt = time.Time{}
	next = errored
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v := testutil.ToFloat64(metrics.PlanguardFailures.WithLabelValues("e", "generic")); v != 1 {
		t.Errorf("(e, generic) = %v", v)
	}
	metrics.PlanguardFailures.Reset()
}

func TestPlanguardRunNowSingleFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		rep := pgReport()
		pg := NewPlanguard(func(context.Context) (*planguard.Report, error) {
			calls.Add(1)
			time.Sleep(time.Minute)
			return rep, nil
		})
		var wg sync.WaitGroup
		got := make([]*planguard.Report, 5)
		for i := range got {
			wg.Go(func() {
				r, err := pg.RunNow(context.Background())
				if err != nil {
					t.Error(err)
				}
				got[i] = r
			})
		}
		wg.Wait()
		if n := calls.Load(); n != 1 {
			t.Fatalf("run called %d times, want 1", n)
		}
		for i, r := range got {
			if r != rep {
				t.Errorf("caller %d got %p, want %p", i, r, rep)
			}
		}
	})
}

func TestPlanguardRunNowRateLimited(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		pg := NewPlanguard(func(context.Context) (*planguard.Report, error) {
			calls.Add(1)
			return pgReport(), nil
		})
		first, err := pg.RunNow(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(freshMinGap - time.Second)
		again, err := pg.RunNow(context.Background())
		if err != nil || again != first || calls.Load() != 1 {
			t.Fatalf("RunNow inside the gap ran again: calls %d, same %v, err %v", calls.Load(), again == first, err)
		}
		time.Sleep(time.Second)
		if _, err := pg.RunNow(context.Background()); err != nil || calls.Load() != 2 {
			t.Fatalf("RunNow after the gap: calls %d, err %v", calls.Load(), err)
		}
	})
}

func TestPlanguardRunNowOutlivesCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var runCtxErr error
		pg := NewPlanguard(func(ctx context.Context) (*planguard.Report, error) {
			time.Sleep(time.Minute)
			runCtxErr = ctx.Err()
			return pgReport(), nil
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := pg.RunNow(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("RunNow = %v, want the caller's deadline", err)
		}
		synctest.Wait()
		time.Sleep(time.Minute)
		synctest.Wait()
		if runCtxErr != nil {
			t.Errorf("the run's context ended with its caller: %v", runCtxErr)
		}
		if _, ok := pg.Last(); !ok {
			t.Error("the run's report was not kept")
		}
	})
}
