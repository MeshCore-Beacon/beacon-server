// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// Advisory lock classes, taken as pg_try_advisory_lock(class, hashtext(current_schema())), so instances sharing a
	// schema serialise and schemas (tests) do not. Distinct from the migration key and the rollup lock (25189, 1).
	cmiReconcileLockKey int32 = 0x636d69 // "cmi"
	// Reserved for the packet_sites read model.
	packetSitesReconcileLockKey int32 = 0x7073 // "ps"
)

// ErrPreBootWriters means a transaction that began before boot still holds an xid: rows it writes may bypass the
// read models, so the boot repair waits for it.
var ErrPreBootWriters = errors.New("transactions from before boot still hold an xid")

// DatabaseNow returns statement_timestamp() from the database, so the boot cut-off and xact_start share one clock.
func DatabaseNow(ctx context.Context, pool *pgxpool.Pool) (time.Time, error) {
	var now time.Time
	err := pool.QueryRow(ctx, "SELECT statement_timestamp()").Scan(&now)
	return now, err
}

// WaitForPreBootWriters polls every 500 ms until no other session of this database that started its transaction
// before bootTS holds an xid; after maxWait it returns ErrPreBootWriters (wrapped with the count and pids).
// A session of another role is visible only to a privileged role; such a writer is not waited for, and the
// periodic reconcile repairs what it wrote.
func WaitForPreBootWriters(ctx context.Context, pool *pgxpool.Pool, bootTS time.Time, maxWait time.Duration) error {
	deadline := time.Now().Add(maxWait)
	for {
		var n int
		var pids []int32
		if err := pool.QueryRow(ctx, `SELECT count(*), coalesce(array_agg(pid ORDER BY pid), '{}') FROM pg_stat_activity
WHERE datname = current_database() AND backend_xid IS NOT NULL AND xact_start < $1 AND pid <> pg_backend_pid()`, bootTS).Scan(&n, &pids); err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%w: %d session(s), pids %v", ErrPreBootWriters, n, pids)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(min(500*time.Millisecond, time.Until(deadline))):
		}
	}
}

// TrustPolicy says how many repairing runs in a row drop a read model's trust and how many clean runs restore it.
type TrustPolicy struct{ DropAfterDirty, RestoreAfterClean int }

// CmiTrustPolicy: an in-flight Add can make one run repair a pair, so one run never flips trust.
var CmiTrustPolicy = TrustPolicy{DropAfterDirty: 3, RestoreAfterClean: 2}

// Trust says whether this process may serve pages from a read model. It is false until Earn, false whenever the
// operator disabled the model, false after DropAfterDirty repairing runs in a row until RestoreAfterClean clean
// runs, and false from a writer's Invalidate until a reconcile that began after it has finished. Every change sets
// beacon_readmodel_trusted{model}.
type Trust struct {
	model   string
	enabled bool
	policy  TrustPolicy

	mu              sync.Mutex
	earned, dropped bool
	dirty, clean    int
	gen             uint64 // writer invalidations so far
	repairedGen     uint64 // gen covered by the last finished reconcile
	trusted         atomic.Bool
}

// NewTrust returns an untrusted model; enabled=false keeps it untrusted for the life of the process.
func NewTrust(model string, enabled bool, p TrustPolicy) *Trust {
	t := &Trust{model: model, enabled: enabled, policy: p}
	t.mu.Lock()
	t.update()
	t.mu.Unlock()
	return t
}

// Trusted reports whether pages may be served from the model. A nil *Trust is untrusted.
func (t *Trust) Trusted() bool { return t != nil && t.trusted.Load() }

// Model returns the model's metric label.
func (t *Trust) Model() string { return t.model }

// Earn records that the boot repair, the backfill and the verification are done.
func (t *Trust) Earn() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.earned = true
	t.update()
}

// ObserveRun applies the drop and restore policy to one periodic reconcile that repaired that many rows. Runs before
// Earn do not count.
func (t *Trust) ObserveRun(repaired int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.earned {
		return
	}
	if repaired > 0 {
		t.dirty, t.clean = t.dirty+1, 0
		if t.dirty >= t.policy.DropAfterDirty {
			t.dropped = true
		}
	} else {
		t.dirty, t.clean = 0, t.clean+1
		if t.dropped && t.clean >= t.policy.RestoreAfterClean {
			t.dropped = false
		}
	}
	t.update()
}

// Invalidate drops trust at once: a writer left a pair the model is missing (a failed Add, a message stored without
// its sites). A reconcile that begins after this call repairs it. Nil-safe.
func (t *Trust) Invalidate(reason string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.gen++
	if t.trusted.Load() {
		slog.Warn("read model untrusted until the next reconcile", "component", "readmodel", "model", t.model, "reason", reason)
	}
	t.update()
}

// Generation returns the invalidation count; a reconcile reads it before it starts and passes it to Repaired.
func (t *Trust) Generation() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.gen
}

// Repaired records a finished reconcile that began at generation gen: it covers every invalidation up to gen.
func (t *Trust) Repaired(gen uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.repairedGen = max(t.repairedGen, gen)
	t.update()
}

// update recomputes trusted and the gauge; t.mu is held.
func (t *Trust) update() {
	on := t.enabled && t.earned && !t.dropped && t.repairedGen >= t.gen
	t.trusted.Store(on)
	v := 0.0
	if on {
		v = 1
	}
	metrics.ReadModelTrusted.WithLabelValues(t.model).Set(v)
}

// ReadModels holds this process's trust in each read model. The ingest, maintenance and API stores share one, so a
// writer's failure is seen by the readers at once.
type ReadModels struct{ Cmi, PacketSites *Trust }

// WithReadModels sets the read models a store reads from and reports writer failures to.
func WithReadModels(rm *ReadModels) Option {
	return func(s *Store) { s.readModels = rm }
}

// cmiTrusted is nil-safe: a nil *ReadModels is untrusted.
func (rm *ReadModels) cmiTrusted() bool { return rm != nil && rm.Cmi.Trusted() }

// invalidateCmi is nil-safe.
func (rm *ReadModels) invalidateCmi(reason string) {
	if rm != nil {
		rm.Cmi.Invalidate(reason)
	}
}
