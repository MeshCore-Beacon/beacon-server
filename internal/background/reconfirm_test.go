// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type reconfirmStore struct {
	budgets []int
	err     error
}

func (s *reconfirmStore) ReconfirmSweep(ctx context.Context, budget int) (db.ReconfirmResult, error) {
	if _, ok := ctx.Deadline(); ok {
		return db.ReconfirmResult{}, errors.New("sweep given a per-call deadline")
	}
	s.budgets = append(s.budgets, budget)
	return db.ReconfirmResult{Scanned: int64(budget)}, s.err
}

func TestReconfirmTaskPacing(t *testing.T) {
	for _, tc := range []struct {
		rowsPerHour int
		tick        time.Duration
		budget      int
	}{
		{81000, 5 * time.Minute, 6750},
		{12000, 5 * time.Minute, 1000},
		{100, 5 * time.Minute, 8},
		{1, 5 * time.Minute, 1},
	} {
		t.Run(fmt.Sprintf("%d per hour", tc.rowsPerHour), func(t *testing.T) {
			s := &reconfirmStore{}
			task := ReconfirmTask(s, tc.rowsPerHour, tc.tick)
			if task.Name != "reconfirm" || task.Interval != tc.tick {
				t.Fatalf("task %q every %s, want reconfirm every %s", task.Name, task.Interval, tc.tick)
			}
			for range 2 {
				if err := task.Run(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if len(s.budgets) != 2 || s.budgets[0] != tc.budget || s.budgets[1] != tc.budget {
				t.Fatalf("sweep budgets %v, want one call of %d per run", s.budgets, tc.budget)
			}
		})
	}
	if ReconfirmTick != 5*time.Minute {
		t.Fatalf("ReconfirmTick = %s, want 5m", ReconfirmTick)
	}
}

func TestReconfirmTaskReturnsSweepError(t *testing.T) {
	want := errors.New("database unavailable")
	s := &reconfirmStore{err: want}
	err := ReconfirmTask(s, 81000, ReconfirmTick).Run(context.Background())
	if !errors.Is(err, want) || err.Error() != "routes: database unavailable" {
		t.Fatalf("err = %v, want routes: %v", err, want)
	}
	if len(s.budgets) != 1 {
		t.Fatalf("sweep called %d times, want 1", len(s.budgets))
	}
}

type deletingReconfirmer struct{}

func (deletingReconfirmer) ReconfirmSweep(context.Context, int) (db.ReconfirmResult, error) {
	return db.ReconfirmResult{Scanned: 10, Deleted: 4}, nil
}

func TestReconfirmTaskCountsDeletedRows(t *testing.T) {
	before := testutil.ToFloat64(metrics.JobRows.WithLabelValues("reconfirm"))
	if err := ReconfirmTask(deletingReconfirmer{}, 12000, ReconfirmTick).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(metrics.JobRows.WithLabelValues("reconfirm")) - before; got != 4 {
		t.Errorf("reconfirm rows +%v, want +4", got)
	}
}
