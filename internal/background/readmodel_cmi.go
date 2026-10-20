// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
)

// CmiInterval is how often channel_message_iatas is reconciled.
const CmiInterval = 5 * time.Minute

const (
	cmiModel          = "cmi"
	cmiBackfillBudget = 30 * time.Second // per Backfill call, so a stop request is honoured between batches
	cmiBusyRetry      = 5 * time.Second
)

// CmiSession is the part of db.CmiSession the jobs use.
type CmiSession interface {
	State() db.CmiState
	Init(ctx context.Context) (db.CmiState, error)
	Reconcile(ctx context.Context) (int64, error)
	Backfill(ctx context.Context, budget time.Duration) (db.CmiState, error)
	Verify(ctx context.Context) (db.CmiVerification, error)
	Close(ctx context.Context)
}

// CmiJobs repairs channel_message_iatas at boot and every Interval, and decides this process's trust in it.
type CmiJobs struct {
	Begin    func(ctx context.Context) (CmiSession, bool, error)
	Wait     func(ctx context.Context) error // db.WaitForPreBootWriters bound to the boot timestamp
	Trust    *db.Trust
	Interval time.Duration

	booted    atomic.Bool
	busyRetry time.Duration // 0 = cmiBusyRetry
}

// sleep waits d or until ctx ends; it reports whether ctx is still live.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// Boot waits until no transaction from before boot can still write, then repairs the table, finishes the
// backfill and, the first time ever, verifies it; then trust is earned. Until then every page is served by the
// original statements. A failure is logged and the repair retried after Interval. Boot returns once the repair is
// done (trust earned, or a failed first verification left to the periodic task) or ctx ends; main runs it in a
// goroutine.
func (j *CmiJobs) Boot(ctx context.Context) {
	for {
		err := j.Wait(ctx)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, db.ErrPreBootWriters) {
			slog.Warn("cmi boot repair waits for transactions from before boot", "component", "readmodel", "error", err)
			continue
		}
		slog.Error("cmi boot repair: pre-boot writer check failed", "component", "readmodel", "error", err)
		if !sleep(ctx, j.Interval) {
			return
		}
	}
	for {
		done, err := j.bootOnce(ctx)
		if done || ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.Error("cmi boot repair failed; retrying", "component", "readmodel", "error", err)
		}
		if !sleep(ctx, j.Interval) {
			return
		}
	}
}

func (j *CmiJobs) bootOnce(ctx context.Context) (bool, error) {
	s, err := j.begin(ctx, true)
	if err != nil || s == nil {
		return false, err
	}
	defer s.Close(context.Background())
	gen := j.Trust.Generation()
	st, err := s.Init(ctx)
	if err != nil {
		return false, err
	}
	if _, err := j.reconcile(ctx, s); err != nil {
		return false, err
	}
	for !st.Complete {
		if st, err = s.Backfill(ctx, cmiBackfillBudget); err != nil {
			return false, err
		}
		slog.Info("cmi backfill progress", "component", "readmodel", "backfilled_to_id", st.BackfilledToID, "max_id", st.BackfillMaxID)
	}
	j.booted.Store(true)
	if st.FirstVerifiedAt == nil {
		v, err := s.Verify(ctx)
		if err != nil {
			return true, fmt.Errorf("cmi verification: %w", err)
		}
		if !v.OK() {
			return true, nil // the task verifies again every interval
		}
	}
	j.Trust.Earn()
	j.Trust.Repaired(gen)
	slog.Info("cmi trusted: region and scope message pages read channel_message_iatas", "component", "readmodel", "trusted", j.Trust.Trusted())
	return true, nil
}

// begin takes the lock; at boot it retries a busy lock, in the task it skips the run (nil session).
func (j *CmiJobs) begin(ctx context.Context, retry bool) (CmiSession, error) {
	for {
		s, ok, err := j.Begin(ctx)
		if err != nil {
			return nil, err
		}
		if ok {
			return s, nil
		}
		if !retry {
			return nil, nil
		}
		wait := j.busyRetry
		if wait == 0 {
			wait = cmiBusyRetry
		}
		if !sleep(ctx, wait) {
			return nil, ctx.Err()
		}
	}
}

// reconcile runs one reconcile and publishes its rows and watermark.
func (j *CmiJobs) reconcile(ctx context.Context, s CmiSession) (int64, error) {
	n, err := s.Reconcile(ctx)
	if err != nil {
		return n, err
	}
	metrics.ReadModelRepaired.WithLabelValues(cmiModel).Add(float64(n))
	metrics.SetReadModelWatermark(cmiModel, time.Now())
	if n > 0 {
		slog.Warn(fmt.Sprintf("cmi reconcile repaired %d pair(s): a writer bypassed the read model", n), "component", "readmodel")
	}
	return n, nil
}

// Task reconciles every Interval once Boot has repaired the table. Its rows feed the trust policy; a run that began
// after a writer's failure restores trust that failure dropped; a never-verified table is verified each run until
// it passes. A busy lock skips the run.
func (j *CmiJobs) Task() Task {
	return Task{
		Name:     "readmodel.cmi",
		Interval: j.Interval,
		Run: func(ctx context.Context) error {
			if !j.booted.Load() {
				return nil
			}
			gen := j.Trust.Generation()
			s, err := j.begin(ctx, false)
			if err != nil || s == nil {
				return err
			}
			defer s.Close(context.Background())
			st, err := s.Init(ctx)
			if err != nil {
				return err
			}
			n, err := j.reconcile(ctx, s)
			if err != nil {
				return err
			}
			j.Trust.ObserveRun(n)
			if !st.Complete {
				_, err := s.Backfill(ctx, cmiBackfillBudget)
				return err
			}
			if st.FirstVerifiedAt == nil {
				v, err := s.Verify(ctx)
				if err != nil {
					return err
				}
				if !v.OK() {
					return nil
				}
				j.Trust.Earn()
			}
			j.Trust.Repaired(gen)
			return nil
		},
	}
}

// CmiReadOnlyTrust earns trust on a read-only server when the persisted state is complete and verified and the
// exact check over the newest messages is clean, and drops it otherwise. It writes nothing.
func CmiReadOnlyTrust(ctx context.Context, check func(context.Context) (db.CmiState, db.CmiVerification, error), trust *db.Trust) {
	gen := trust.Generation()
	st, v, err := check(ctx)
	switch {
	case err != nil:
		slog.Error("cmi read-only check failed", "component", "readmodel", "error", err)
	case !st.Complete || st.FirstVerifiedAt == nil:
		slog.Info("cmi not trusted on this read-only server: backfill or first verification not done", "component", "readmodel", "complete", st.Complete)
	case !v.OK():
		slog.Warn("cmi not trusted on this read-only server: the newest messages fail the exact check", "component", "readmodel", "missing", v.Missing, "extra", v.Extra)
	default:
		if !trust.Trusted() {
			slog.Info("cmi trusted on this read-only server: region and scope message pages read channel_message_iatas", "component", "readmodel")
		}
		trust.Earn()
		trust.Repaired(gen)
		return
	}
	trust.Invalidate("read-only check not clean")
}

// CmiReadOnlyWatch runs CmiReadOnlyTrust now and every interval until ctx ends.
func CmiReadOnlyWatch(ctx context.Context, check func(context.Context) (db.CmiState, db.CmiVerification, error), trust *db.Trust, interval time.Duration) {
	for {
		CmiReadOnlyTrust(ctx, check, trust)
		if !sleep(ctx, interval) {
			return
		}
	}
}
