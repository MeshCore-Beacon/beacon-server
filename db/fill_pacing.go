// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// FillPacing paces the one-off backfills (packet_sites, cmi and observation payload types). After each slice or
// batch a fill sleeps Pause(elapsed), so it is busy at most Duty of the time and a slow disk, which makes each slice
// take longer, stretches the pauses on its own. Pacing changes only when the fills run, never what they write.
type FillPacing struct {
	Duty             float64       // the share of time a fill may be busy, in (0, 1]
	MinPause         time.Duration // the shortest pause after a slice or batch
	PacketSitesSlice int64         // observation ids one packet_sites backfill slice covers
	CmiBatch         int64         // message ids one cmi backfill batch covers
}

// DefaultFillPacing is safe for a small server on a rotational disk: busy at most half the time.
func DefaultFillPacing() FillPacing {
	return FillPacing{Duty: 0.5, MinPause: 100 * time.Millisecond, PacketSitesSlice: packetSitesSlice, CmiBatch: cmiBackfillBatch}
}

// WithFillPacing sets the store's backfill pacing; without it the store uses DefaultFillPacing.
func WithFillPacing(p FillPacing) Option {
	return func(s *Store) { s.fill = &p }
}

// FillPacing returns the store's backfill pacing.
func (s *Store) FillPacing() FillPacing {
	if s.fill == nil {
		return DefaultFillPacing()
	}
	return *s.fill
}

// Pause is the sleep after a slice that ran for elapsed: max(MinPause, elapsed × (1 − Duty) / Duty). A Duty
// outside (0, 1) adds nothing to MinPause.
func (p FillPacing) Pause(elapsed time.Duration) time.Duration {
	d := p.MinPause
	if p.Duty > 0 && p.Duty < 1 {
		d = max(d, time.Duration(float64(elapsed)*(1-p.Duty)/p.Duty))
	}
	return d
}

// Wait sleeps Pause(elapsed) or until ctx ends; it reports whether ctx is still live.
func (p FillPacing) Wait(ctx context.Context, elapsed time.Duration) bool {
	d := p.Pause(elapsed)
	if d <= 0 {
		return ctx.Err() == nil
	}
	return sleepCtx(ctx, d)
}

// fillProgressEvery is how often a running backfill logs its progress.
const fillProgressEvery = time.Minute

// FillProgress logs a long backfill's progress at most once a minute: the ids done, the ids remaining and an ETA
// from the rate since the first Start. The zero value is ready to use; it is safe for concurrent use.
type FillProgress struct {
	mu         sync.Mutex
	now        func() time.Time // tests replace it
	started    time.Time
	startLeft  int64
	lastLogged time.Time
}

func (p *FillProgress) clock() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

// Start starts the clock with remaining ids left; later calls do nothing.
func (p *FillProgress) Start(remaining int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started.IsZero() {
		p.started = p.clock()
		p.startLeft = remaining
		p.lastLogged = p.started
	}
}

// Due reports whether a minute has passed since the last log (or the start).
func (p *FillProgress) Due() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.started.IsZero() && p.clock().Sub(p.lastLogged) >= fillProgressEvery
}

// ETA is the time to finish remaining ids at the rate since Start; ok is false until some ids are done.
func (p *FillProgress) ETA(remaining int64) (eta time.Duration, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.eta(p.clock(), remaining)
}

func (p *FillProgress) eta(now time.Time, remaining int64) (time.Duration, bool) {
	elapsed, progressed := now.Sub(p.started), p.startLeft-remaining
	if p.started.IsZero() || progressed <= 0 || elapsed <= 0 {
		return 0, false
	}
	return time.Duration(float64(elapsed) * float64(remaining) / float64(progressed)), true
}

// Log logs msg with the ids done and remaining and, once some ids are done, the ETA at the rate since Start. It
// returns the ETA and whether there was one.
func (p *FillProgress) Log(msg string, done, remaining int64, attrs ...any) (time.Duration, bool) {
	p.mu.Lock()
	now := p.clock()
	p.lastLogged = now
	eta, ok := p.eta(now, remaining)
	p.mu.Unlock()
	attrs = append(attrs, "component", "readmodel", "ids_done", done, "ids_remaining", remaining)
	if ok {
		attrs = append(attrs, "eta", eta.Round(time.Second).String())
	}
	slog.Info(msg, attrs...)
	return eta, ok
}

// fillPlanMode is set on every read-model fill and reconcile connection for the session (and reset on Close). With
// automatic plan caching, a prepared fill statement switches to a generic plan after five executions. Built while a
// new read-model table has no statistics, that plan assumes a few thousand rows per slice; measured on 3 million
// observations, the 6th 500,000-id packet_sites slice ran past a 2-minute timeout instead of taking 14 s. Custom
// plans see each slice's real bounds. The maint pool sets the same at connect; this keeps it when a pooler drops
// startup parameters, or when the store runs on another pool.
const fillPlanMode = "SET plan_cache_mode = force_custom_plan"

// fillAnalyzeLockTimeout bounds how long ANALYZE of a read-model table waits for its lock (an autovacuum holds it);
// tests shorten it.
var fillAnalyzeLockTimeout = "10s"

// analyzeIfUnanalyzed ANALYZEs table when it has never been analyzed or vacuumed (reltuples -1): a new read-model
// table, whose fill would otherwise be planned blind. A failure is logged and ignored.
func analyzeIfUnanalyzed(ctx context.Context, conn *pgxpool.Conn, table string) {
	var never bool
	if err := conn.QueryRow(ctx, "SELECT reltuples < 0 FROM pg_class WHERE oid = $1::regclass", table).Scan(&never); err != nil {
		slog.Warn("read-model statistics check failed; the fill goes on", "component", "readmodel", "table", table, "error", err)
		return
	}
	if never {
		analyzeFillTable(ctx, conn, table)
	}
}

// analyzeFillTable runs ANALYZE table with no statement timeout and a bounded lock wait, so the fill's later
// statements are planned on the table's real size. It is cheap: at the default statistics target it samples at most
// 30,000 rows. A failure is logged and ignored: statistics help the plans, the fill does not need them.
func analyzeFillTable(ctx context.Context, conn *pgxpool.Conn, table string) {
	err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = 0"); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '"+fillAnalyzeLockTimeout+"'"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "ANALYZE "+pgx.Identifier{table}.Sanitize())
		return err
	})
	if err != nil {
		slog.Warn("read-model ANALYZE failed; the fill goes on", "component", "readmodel", "table", table, "error", err)
	}
}
