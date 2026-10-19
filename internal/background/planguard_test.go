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
		pg := NewPlanguard(context.Background(), func(context.Context) (*planguard.Report, error) {
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
	pg := NewPlanguard(context.Background(), func(context.Context) (*planguard.Report, error) { return next, nextErr })
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
		pg := NewPlanguard(context.Background(), func(context.Context) (*planguard.Report, error) {
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
		pg := NewPlanguard(context.Background(), func(context.Context) (*planguard.Report, error) {
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
		pg := NewPlanguard(context.Background(), func(ctx context.Context) (*planguard.Report, error) {
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

// Cancelling the base context (server shutdown) ends a fresh run in flight, and a scheduler tick joined to it
// returns instead of waiting out the run.
func TestPlanguardShutdownCancelsFreshRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base, shutdown := context.WithCancel(context.Background())
		var runCtxErr error
		pg := NewPlanguard(base, func(ctx context.Context) (*planguard.Report, error) {
			select {
			case <-ctx.Done():
				runCtxErr = ctx.Err()
				return nil, ctx.Err()
			case <-time.After(freshRunTimeout + time.Hour):
				return pgReport(), nil
			}
		})
		freshErr := make(chan error, 1)
		go func() {
			_, err := pg.RunNow(context.Background())
			freshErr <- err
		}()
		synctest.Wait()
		tickErr := make(chan error, 1)
		go func() { tickErr <- pg.Task(24 * time.Hour).Run(context.Background()) }()
		synctest.Wait()
		start := time.Now()
		shutdown()
		if err := <-tickErr; !errors.Is(err, context.Canceled) {
			t.Errorf("joined tick returned %v, want context.Canceled", err)
		}
		if err := <-freshErr; !errors.Is(err, context.Canceled) {
			t.Errorf("fresh run returned %v, want context.Canceled", err)
		}
		synctest.Wait()
		if !errors.Is(runCtxErr, context.Canceled) {
			t.Errorf("the run's context ended with %v, want context.Canceled", runCtxErr)
		}
		if d := time.Since(start); d != 0 {
			t.Errorf("shutdown waited %v for the run", d)
		}
	})
}

// A caller that gives up stops waiting for a run it joined; the run goes on.
func TestPlanguardJoinHonoursCallerContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		pg := NewPlanguard(context.Background(), func(context.Context) (*planguard.Report, error) {
			calls.Add(1)
			time.Sleep(time.Hour)
			return pgReport(), nil
		})
		go func() { _, _ = pg.RunNow(context.Background()) }()
		synctest.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := pg.Task(24 * time.Hour).Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("joined tick = %v, want its own deadline", err)
		}
		time.Sleep(time.Hour)
		synctest.Wait()
		if calls.Load() != 1 {
			t.Errorf("run called %d times, want 1", calls.Load())
		}
	})
}

// Callers arriving while a run is in flight and right after it ends never start a second run inside the gap.
func TestPlanguardRunNowNoChaining(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		pg := NewPlanguard(context.Background(), func(context.Context) (*planguard.Report, error) {
			calls.Add(1)
			time.Sleep(time.Minute)
			return pgReport(), nil
		})
		var wg sync.WaitGroup
		for i := range 10 {
			wg.Go(func() {
				time.Sleep(time.Duration(i) * 15 * time.Second) // 0 s .. 135 s: during the run and after it
				if _, err := pg.RunNow(context.Background()); err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		if n := calls.Load(); n != 1 {
			t.Errorf("run called %d times, want 1", n)
		}
	})
}

// Inside the gap a failed attempt does not hide the last good report; with no report the error is returned.
func TestPlanguardRunNowFailedAttemptInGap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("no connection")
		good := pgReport()
		var next *planguard.Report
		var nextErr error
		pg := NewPlanguard(context.Background(), func(context.Context) (*planguard.Report, error) { return next, nextErr })

		nextErr = failure
		if _, err := pg.RunNow(context.Background()); !errors.Is(err, failure) {
			t.Fatalf("first attempt = %v", err)
		}
		if _, err := pg.RunNow(context.Background()); !errors.Is(err, failure) {
			t.Errorf("in the gap with no report = %v, want the attempt's error", err)
		}

		time.Sleep(freshMinGap)
		next, nextErr = good, nil
		if rep, err := pg.RunNow(context.Background()); err != nil || rep != good {
			t.Fatalf("good run = %v, %v", rep, err)
		}
		time.Sleep(freshMinGap)
		next, nextErr = nil, failure
		if _, err := pg.RunNow(context.Background()); !errors.Is(err, failure) {
			t.Fatalf("failed run after the gap = %v", err)
		}
		if rep, err := pg.RunNow(context.Background()); err != nil || rep != good {
			t.Errorf("in the gap after a failed attempt = %v, %v; want the last good report", rep, err)
		}
	})
}
