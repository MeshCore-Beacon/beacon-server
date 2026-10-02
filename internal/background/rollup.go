// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
)

// Per-pass budgets keep one pass short; the boot catch-up loops passes instead.
const (
	rollupInterval      = 5 * time.Minute
	rollupMissingBudget = 48
	rollupDirtyBatch    = 24
	rollupDirtyBudget   = 168 // a key import can dirty a week of hours at once
)

// RollupSession is the lock-holding connection a pass works through (see db.RollupSession).
type RollupSession interface {
	RegisterHours(ctx context.Context, since time.Time) error
	MarkPartialHours(ctx context.Context) error
	MissingHours(ctx context.Context, limit int32) ([]time.Time, error)
	DirtyHours(ctx context.Context, limit int32) ([]time.Time, error)
	RollHour(ctx context.Context, hour time.Time) (db.RollOutcome, bool, error)
	Close(ctx context.Context)
}

// Rollup rolls closed hours into the analytics rollup tables. Only the process holding
// the advisory lock does any work; the others skip the pass.
type Rollup struct {
	begin           func(context.Context) (RollupSession, bool, error)
	packetRetention time.Duration
	rollupRetention time.Duration
	now             func() time.Time
}

// NewRollup rolls hours still inside both retentions using store's rollup lock.
func NewRollup(store *db.Store, packetRetention, rollupRetention time.Duration) *Rollup {
	return &Rollup{
		begin: func(ctx context.Context) (RollupSession, bool, error) {
			s, ok, err := store.BeginRollup(ctx)
			if s == nil {
				return nil, ok, err
			}
			return s, ok, err
		},
		packetRetention: packetRetention,
		rollupRetention: rollupRetention,
		now:             time.Now,
	}
}

// Task runs a pass every five minutes.
func (r *Rollup) Task() Task {
	return Task{
		Name:     "analytics_rollup",
		Interval: rollupInterval,
		Run: func(ctx context.Context) error {
			_, _, err := r.Pass(ctx)
			return err
		},
	}
}

// CatchUp runs passes until the backlog fits in one, so a restart fills gaps without
// waiting for the ticker.
func (r *Rollup) CatchUp(ctx context.Context) error {
	for {
		_, more, err := r.Pass(ctx)
		if err != nil || !more {
			return err
		}
	}
}

// registerSince is the first hour worth rolling: raw rows must still exist, and cleanup
// (DeleteOldRollups, hour < now - rollupRetention) must not delete it again next run.
func (r *Rollup) registerSince() time.Time {
	now := r.now()
	since := now.Add(-r.packetRetention)
	if rc := now.Add(-r.rollupRetention); r.rollupRetention > 0 && rc.After(since) {
		since = rc
	}
	if h := since.Truncate(time.Hour); h.Before(since) {
		return h.Add(time.Hour)
	}
	return since
}

// Pass registers and rolls missing hours oldest first, then re-rolls dirty ones. A failed
// hour doesn't stop the pass; its error is returned with the others. rolled counts hours
// attempted; more reports a budget ran out with work likely left.
func (r *Rollup) Pass(ctx context.Context) (rolled int, more bool, err error) {
	s, ok, err := r.begin(ctx)
	if err != nil || !ok {
		return 0, false, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		s.Close(closeCtx)
		cancel()
	}()

	if err := s.RegisterHours(ctx, r.registerSince()); err != nil {
		return 0, false, fmt.Errorf("register hours: %w", err)
	}
	if err := s.MarkPartialHours(ctx); err != nil {
		return 0, false, fmt.Errorf("mark partial hours: %w", err)
	}
	var errs []error
	roll := func(hours []time.Time) {
		for _, h := range hours {
			if ctx.Err() != nil {
				return
			}
			if _, _, err := s.RollHour(ctx, h); err != nil {
				errs = append(errs, fmt.Errorf("roll %s: %w", h.UTC().Format(time.RFC3339), err))
			}
		}
	}
	missing, err := s.MissingHours(ctx, rollupMissingBudget)
	if err != nil {
		return 0, false, fmt.Errorf("missing hours: %w", err)
	}
	roll(missing)
	rolled, more = len(missing), len(missing) == rollupMissingBudget
	// Hours that fail stay queued; never retry one within a pass.
	tried := map[time.Time]bool{}
	for budget := rollupDirtyBudget; budget > 0 && ctx.Err() == nil; {
		dirty, err := s.DirtyHours(ctx, min(rollupDirtyBatch, int32(budget)))
		if err != nil {
			errs = append(errs, fmt.Errorf("dirty hours: %w", err))
			break
		}
		var fresh []time.Time
		for _, h := range dirty {
			if !tried[h] {
				tried[h] = true
				fresh = append(fresh, h)
			}
		}
		if len(fresh) == 0 {
			break
		}
		roll(fresh)
		rolled += len(fresh)
		budget -= len(fresh)
		if budget == 0 {
			more = true
		}
	}
	if err := ctx.Err(); err != nil {
		errs = append(errs, err)
	}
	return rolled, more, errors.Join(errs...)
}
