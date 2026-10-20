// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"log/slog"
	"math"
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

// MinFillDuty is the lowest duty a fill takes: 1 % busy, a pause of 99 times each slice's run time.
const MinFillDuty = 0.01

// MaxFillPause caps any one pause, so a slice that ran very long (or a tiny duty) cannot park a fill for hours.
const MaxFillPause = 10 * time.Minute

// Pause is the sleep after a slice that ran for elapsed: max(MinPause, elapsed × (1 − Duty) / Duty), with Duty
// raised to MinFillDuty and the result capped at MaxFillPause. A Duty of 1 or more, or unset (0), adds nothing to
// MinPause.
func (p FillPacing) Pause(elapsed time.Duration) time.Duration {
	d := min(max(p.MinPause, 0), MaxFillPause)
	if p.Duty > 0 && p.Duty < 1 && elapsed > 0 {
		duty := max(p.Duty, MinFillDuty)
		// In float, so a long slice cannot overflow the conversion; capped before converting back.
		stretch := min(float64(elapsed)*(1-duty)/duty, float64(MaxFillPause))
		d = max(d, time.Duration(stretch))
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

// FillProgress logs a long backfill's progress at most once a minute: the ids done, the ids remaining and an ETA.
// The rate is how far the fill's position (an id it has passed) moved since Start, so the ETA follows the current
// remaining count even when that grows with ingest. The zero value is ready to use; it is safe for concurrent use.
type FillProgress struct {
	mu         sync.Mutex
	now        func() time.Time // tests replace it
	started    time.Time
	startPos   int64
	lastLogged time.Time
}

func (p *FillProgress) clock() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

// Start starts the clock at position pos; later calls do nothing until Reset.
func (p *FillProgress) Start(pos int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started.IsZero() {
		p.started = p.clock()
		p.startPos = pos
		p.lastLogged = p.started
	}
}

// Reset forgets the clock, for a fill that starts over (a rebuild) or has completed.
func (p *FillProgress) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.started, p.startPos, p.lastLogged = time.Time{}, 0, time.Time{}
}

// Due reports whether a minute has passed since the last log (or the start).
func (p *FillProgress) Due() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.started.IsZero() && p.clock().Sub(p.lastLogged) >= fillProgressEvery
}

// ETA is the time to finish remaining ids at the rate the position moved since Start; ok is false until it moved.
func (p *FillProgress) ETA(pos, remaining int64) (eta time.Duration, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.eta(p.clock(), pos, remaining)
}

func (p *FillProgress) eta(now time.Time, pos, remaining int64) (time.Duration, bool) {
	elapsed, moved := now.Sub(p.started), pos-p.startPos
	if p.started.IsZero() || moved <= 0 || elapsed <= 0 || remaining < 0 {
		return 0, false
	}
	return time.Duration(min(float64(elapsed)*float64(remaining)/float64(moved), float64(math.MaxInt64))), true
}

// Log logs msg with the ids done and remaining and, once the position moved, the ETA. It returns the ETA and
// whether there was one.
func (p *FillProgress) Log(msg string, pos, done, remaining int64, attrs ...any) (time.Duration, bool) {
	p.mu.Lock()
	now := p.clock()
	p.lastLogged = now
	eta, ok := p.eta(now, pos, remaining)
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

// analyzeHook, when set by a test, sees every read-model ANALYZE before it runs.
var analyzeHook func(table string)

// fillAnalyzeLockTimeout bounds how long ANALYZE of a read-model table waits for its lock (an autovacuum holds it);
// tests shorten it.
var fillAnalyzeLockTimeout = "10s"

// analyzeAtStart ANALYZEs table when a session starts and finds it never analyzed but holding rows (those the
// writer stored since the table was created). An empty table waits for the first slice: ANALYZE would only record
// it as empty.
func analyzeAtStart(ctx context.Context, conn *pgxpool.Conn, table string) {
	if !neverAnalyzed(ctx, conn, table) {
		return
	}
	var rows bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+pgx.Identifier{table}.Sanitize()+")").Scan(&rows); err != nil {
		slog.Warn("read-model statistics check failed; the fill goes on", "component", "readmodel", "table", table, "error", err)
		return
	}
	if rows {
		analyzeFillTable(ctx, conn, table)
	}
}

// analyzeAfterFirstSlice ANALYZEs table after a session's first backfill slice or batch if it has still never been
// analyzed. A table with statistics is left to autovacuum: re-ANALYZE of a large table is a burst of its own.
func analyzeAfterFirstSlice(ctx context.Context, conn *pgxpool.Conn, table string) {
	if neverAnalyzed(ctx, conn, table) {
		analyzeFillTable(ctx, conn, table)
	}
}

// neverAnalyzed reports whether table has never been analyzed or vacuumed (reltuples -1): a new read-model table,
// whose fill would otherwise be planned blind. A failed check is logged and reads as false.
func neverAnalyzed(ctx context.Context, conn *pgxpool.Conn, table string) bool {
	var never bool
	if err := conn.QueryRow(ctx, "SELECT reltuples < 0 FROM pg_class WHERE oid = $1::regclass", table).Scan(&never); err != nil {
		slog.Warn("read-model statistics check failed; the fill goes on", "component", "readmodel", "table", table, "error", err)
		return false
	}
	return never
}

// analyzeFillTable runs ANALYZE table with no statement timeout and a bounded lock wait, so the fill's later
// statements are planned on the table's real size. It is cheap: at the default statistics target it samples at most
// 30,000 rows. A failure is logged and ignored: statistics help the plans, the fill does not need them.
func analyzeFillTable(ctx context.Context, conn *pgxpool.Conn, table string) {
	if analyzeHook != nil {
		analyzeHook(table)
	}
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
