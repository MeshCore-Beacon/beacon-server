// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
)

type rollupStub struct {
	calls   []string
	since   time.Time
	missing [][]time.Time // one batch per pass
	dirty   []time.Time
	failAt  map[time.Time]error
	done    map[time.Time]bool // rolled dirty hours leave the queue
	closed  int
}

func (s *rollupStub) RegisterHours(_ context.Context, since time.Time) error {
	s.calls = append(s.calls, "register")
	s.since = since
	return nil
}
func (s *rollupStub) MarkPartialHours(context.Context) error {
	s.calls = append(s.calls, "partial")
	return nil
}
func (s *rollupStub) MissingHours(_ context.Context, limit int32) ([]time.Time, error) {
	s.calls = append(s.calls, fmt.Sprintf("missing(%d)", limit))
	if len(s.missing) == 0 {
		return nil, nil
	}
	batch := s.missing[0]
	s.missing = s.missing[1:]
	return batch, nil
}
func (s *rollupStub) DirtyHours(_ context.Context, limit int32) ([]time.Time, error) {
	s.calls = append(s.calls, fmt.Sprintf("dirty(%d)", limit))
	var out []time.Time
	for _, h := range s.dirty {
		if !s.done[h] && int32(len(out)) < limit {
			out = append(out, h)
		}
	}
	return out, nil
}
func (s *rollupStub) RollHour(_ context.Context, h time.Time) (db.RollOutcome, bool, error) {
	s.calls = append(s.calls, "roll "+h.Format("15"))
	if err := s.failAt[h]; err != nil {
		return db.RollSkipped, false, err
	}
	if s.done == nil {
		s.done = map[time.Time]bool{}
	}
	s.done[h] = true
	return db.RollComplete, true, nil
}
func (s *rollupStub) Close(context.Context) { s.closed++ }

func newTestRollup(s *rollupStub, locked bool, beginErr error) *Rollup {
	now := time.Date(2026, 1, 8, 12, 0, 0, 0, time.UTC)
	return &Rollup{
		begin: func(context.Context) (RollupSession, bool, error) {
			if beginErr != nil || !locked {
				return nil, false, beginErr
			}
			return s, true, nil
		},
		packetRetention: 7 * 24 * time.Hour,
		rollupRetention: 90 * 24 * time.Hour,
		now:             func() time.Time { return now },
	}
}

func hours(hs ...int) []time.Time {
	out := make([]time.Time, len(hs))
	for i, h := range hs {
		out[i] = time.Date(2026, 1, 8, h, 0, 0, 0, time.UTC)
	}
	return out
}

func TestRollupPass(t *testing.T) {
	s := &rollupStub{missing: [][]time.Time{hours(1, 2, 3)}, dirty: hours(0, 4)}
	s.failAt = map[time.Time]error{hours(2)[0]: errors.New("boom"), hours(4)[0]: errors.New("stuck")}
	rolled, more, err := newTestRollup(s, true, nil).Pass(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Errorf("err = %v, want the failed hour's error", err)
	}
	if rolled != 5 || more {
		t.Errorf("rolled = %d more = %v, want 5, false", rolled, more)
	}
	// A failing dirty hour stays queued but isn't retried within the pass.
	want := []string{"register", "partial", "missing(48)", "roll 01", "roll 02", "roll 03", "dirty(24)", "roll 00", "roll 04", "dirty(24)"}
	if !reflect.DeepEqual(s.calls, want) {
		t.Errorf("calls = %v, want %v", s.calls, want)
	}
	if !s.since.Equal(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("registered since %s, want now - packet retention", s.since)
	}
	if s.closed != 1 {
		t.Errorf("session closed %d times", s.closed)
	}
}

func TestRollupRegisterSince(t *testing.T) {
	now := time.Date(2026, 1, 8, 12, 20, 0, 0, time.UTC)
	for _, tc := range []struct {
		name            string
		packet, rollups time.Duration
		want            time.Time
	}{
		{"packet retention shorter", 7 * 24 * time.Hour, 90 * 24 * time.Hour, time.Date(2026, 1, 1, 13, 0, 0, 0, time.UTC)},
		// Hours cleanup would delete next run must not be registered again.
		{"rollup retention shorter", 30 * 24 * time.Hour, 24 * time.Hour, time.Date(2026, 1, 7, 13, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &Rollup{packetRetention: tc.packet, rollupRetention: tc.rollups, now: func() time.Time { return now }}
			got := r.registerSince()
			if !got.Equal(tc.want) {
				t.Errorf("registerSince = %s, want %s", got, tc.want)
			}
			if got.Before(now.Add(-tc.rollups)) {
				t.Error("registers an hour DeleteOldRollups would remove")
			}
		})
	}
}

func TestRollupPassLockHeldElsewhere(t *testing.T) {
	s := &rollupStub{}
	if rolled, _, err := newTestRollup(s, false, nil).Pass(context.Background()); err != nil || rolled != 0 || len(s.calls) != 0 {
		t.Errorf("rolled %d err %v calls %v; want a silent skip", rolled, err, s.calls)
	}
	failure := errors.New("no connection")
	if _, _, err := newTestRollup(s, true, failure).Pass(context.Background()); !errors.Is(err, failure) {
		t.Errorf("err = %v, want %v", err, failure)
	}
}

func TestRollupCatchUp(t *testing.T) {
	full := make([]int, rollupMissingBudget)
	for i := range full {
		full[i] = i % 24
	}
	s := &rollupStub{missing: [][]time.Time{hours(full...), hours(full...), hours(5)}}
	if err := newTestRollup(s, true, nil).CatchUp(context.Background()); err != nil {
		t.Fatal(err)
	}
	passes := 0
	for _, c := range s.calls {
		if c == "register" {
			passes++
		}
	}
	if passes != 3 || s.closed != 3 {
		t.Errorf("passes %d, closes %d; want 3 (two full batches, then a short one)", passes, s.closed)
	}
}

func TestRollupDrainsDirtyBacklog(t *testing.T) {
	var backlog []time.Time
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 200 {
		backlog = append(backlog, start.Add(time.Duration(i)*time.Hour))
	}
	s := &rollupStub{dirty: backlog}
	r := newTestRollup(s, true, nil)
	rolled, more, err := r.Pass(context.Background())
	if err != nil || rolled != rollupDirtyBudget || !more {
		t.Fatalf("first pass rolled %d more %v err %v; want %d, true", rolled, more, err, rollupDirtyBudget)
	}
	if err := r.CatchUp(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(s.done) != len(backlog) {
		t.Errorf("drained %d of %d dirty hours", len(s.done), len(backlog))
	}
}

func TestRollupTaskInterval(t *testing.T) {
	task := newTestRollup(&rollupStub{}, true, nil).Task()
	if task.Name != "analytics_rollup" || task.Interval != 5*time.Minute {
		t.Errorf("task %q every %s", task.Name, task.Interval)
	}
}
