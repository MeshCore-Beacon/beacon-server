// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
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
