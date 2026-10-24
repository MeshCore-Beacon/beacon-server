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
)

func TestScheduler_RunsTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32

		tasks := []Task{
			{
				Name:     "test-task",
				Interval: 10 * time.Millisecond,
				Run: func(ctx context.Context) error {
					calls.Add(1)
					return nil
				},
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s := New(tasks)
		go s.Start(ctx)

		time.Sleep(50 * time.Millisecond)
		synctest.Wait()
		if calls.Load() == 0 {
			t.Error("expected task to have run at least once")
		}
	})
}

func TestScheduler_StopsOnContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32

		tasks := []Task{
			{
				Name:     "test-task",
				Interval: 10 * time.Millisecond,
				Run: func(ctx context.Context) error {
					calls.Add(1)
					return nil
				},
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		s := New(tasks)
		go s.Start(ctx)

		time.Sleep(30 * time.Millisecond)
		synctest.Wait()
		cancel()
		synctest.Wait()

		snapshot := calls.Load()
		time.Sleep(30 * time.Millisecond)

		if calls.Load() != snapshot {
			t.Errorf("expected task to stop after cancel, got %d more calls", calls.Load()-snapshot)
		}
	})
}

func TestScheduler_MultipleTasksRunIndependently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls1, calls2 atomic.Int32

		tasks := []Task{
			{
				Name:     "task1",
				Interval: 10 * time.Millisecond,
				Run: func(ctx context.Context) error {
					calls1.Add(1)
					return nil
				},
			},
			{
				Name:     "task2",
				Interval: 10 * time.Millisecond,
				Run: func(ctx context.Context) error {
					calls2.Add(1)
					return nil
				},
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s := New(tasks)
		go s.Start(ctx)

		time.Sleep(50 * time.Millisecond)
		synctest.Wait()
		if calls1.Load() == 0 {
			t.Error("expected task1 to have run")
		}
		if calls2.Load() == 0 {
			t.Error("expected task2 to have run")
		}
	})
}

// histogramCount reads the sample count of metric{job=job} from the registry.
func histogramCount(t *testing.T, metric, job string) uint64 {
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
				if l.GetName() == "job" && l.GetValue() == job {
					return m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}

func TestSchedulerRecordsJobDuration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const job = "duration-test-task"
		before := histogramCount(t, "beacon_job_duration_seconds", job)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := New([]Task{{Name: job, Interval: time.Second, Run: func(ctx context.Context) error {
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
			}
			return errors.New("fails, still timed")
		}}})
		go s.Start(ctx)
		time.Sleep(3500 * time.Millisecond)
		synctest.Wait()
		if got := histogramCount(t, "beacon_job_duration_seconds", job); got != before+1 {
			t.Errorf("%d runs observed, want %d", got-before, 1)
		}
		cancel()
		synctest.Wait()
	})
}

// Start returns only once a task that was running when ctx ended has returned, so shutdown can wait for it.
func TestStartWaitsForRunningTask(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s := New([]Task{{Name: "slow", Interval: time.Millisecond, Run: func(context.Context) error {
		once.Do(func() { close(started) })
		<-release
		return nil
	}}})
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(returned)
	}()
	<-started
	cancel()
	select {
	case <-returned:
		t.Fatal("Start returned while a task was still running")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after the task finished")
	}
}
