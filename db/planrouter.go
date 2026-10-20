// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"sync"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/guards"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PlanRouter runs each API statement on the pool its plan class and the layout pick:
//
//	class    custom_all   split        single
//	Legacy   api_legacy   api_legacy   api_legacy   (ungated, as given)
//	G        api_custom   api_cached   api_legacy   (gated, named statement)
//	C        api_custom   api_custom   api_legacy   (gated, unnamed statement)
//
// Class C statements always run unnamed (pgx.QueryExecModeCacheDescribe), so Postgres plans them
// with their values on every call. They never reach api_cached, whose force_generic_plan would
// make even an unnamed statement generic.
type PlanRouter struct {
	legacy, custom, cached routerPool
	layout                 planpolicy.Layout
	gate                   guards.Gate
	classOf                func(sql string) planpolicy.Class
}

var _ sqlc.DBTX = (*PlanRouter)(nil)

// routerPool is the part of a pool the router uses.
type routerPool interface {
	acquire(ctx context.Context) (routerConn, error)
	name() string
}

// routerConn is one acquired connection.
type routerConn interface {
	sqlc.DBTX
	Release()
}

type pgxRouterPool struct {
	pool *pgxpool.Pool
	n    string
}

func (p pgxRouterPool) acquire(ctx context.Context) (routerConn, error) { return p.pool.Acquire(ctx) }
func (p pgxRouterPool) name() string                                    { return p.n }

// wrapPool returns nil for a pool the layout did not open, so a wrong route fails loudly.
func wrapPool(p *pgxpool.Pool, name string) routerPool {
	if p == nil {
		return nil
	}
	return pgxRouterPool{pool: p, n: name}
}

// NewPlanRouter routes over p's API pools. A nil gate admits everything.
func NewPlanRouter(p *Pools, layout planpolicy.Layout, gate guards.Gate) *PlanRouter {
	return newPlanRouter(wrapPool(p.APILegacy, PoolAPILegacy), wrapPool(p.APICustom, PoolAPICustom),
		wrapPool(p.APICached, PoolAPICached), layout, gate, nil)
}

func newPlanRouter(legacy, custom, cached routerPool, layout planpolicy.Layout, gate guards.Gate, classOf func(string) planpolicy.Class) *PlanRouter {
	if gate == nil {
		gate = guards.NopGate
	}
	if classOf == nil {
		classOf = planpolicy.ClassOf
	}
	return &PlanRouter{legacy: legacy, custom: custom, cached: cached, layout: layout, gate: gate,
		classOf: func(sql string) planpolicy.Class { return classOf(sqlcName(sql)) }}
}

// route returns the pool, the gate and the args for one statement.
func (r *PlanRouter) route(sql string, args []any) (routerPool, guards.Gate, []any) {
	switch r.classOf(sql) {
	case planpolicy.ClassLegacy:
		return r.legacy, guards.NopGate, args
	case planpolicy.ClassG:
		switch r.layout {
		case planpolicy.Single:
			return r.legacy, r.gate, args
		case planpolicy.Split:
			return r.cached, r.gate, args
		}
		return r.custom, r.gate, args
	}
	args = append([]any{pgx.QueryExecModeCacheDescribe}, args...)
	if r.layout == planpolicy.Single {
		return r.legacy, r.gate, args
	}
	return r.custom, r.gate, args
}

// stmt is one statement holding a ticket and a connection; end releases both once.
type stmt struct {
	conn   routerConn
	ctx    context.Context
	args   []any
	once   sync.Once
	ticket guards.Ticket
	cancel context.CancelFunc
}

// begin admits the statement, acquires its connection and starts its context. On error,
// everything it took is already given back.
func (r *PlanRouter) begin(ctx context.Context, sql string, args []any) (*stmt, error) {
	pool, gate, args := r.route(sql, args)
	t, err := gate.Admit(ctx)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			t.Finish()
		}
	}()
	conn, err := acquire(ctx, pool, t)
	t.Acquired(err)
	if err != nil {
		return nil, err
	}
	sctx, cancel := t.Start(ctx)
	ok = true
	return &stmt{conn: conn, ctx: sctx, args: args, ticket: t, cancel: cancel}, nil
}

// acquire waits at most the ticket's acquire timeout; 0 means only ctx bounds the wait.
func acquire(ctx context.Context, pool routerPool, t guards.Ticket) (routerConn, error) {
	if d := t.AcquireTimeout(); d > 0 {
		actx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return pool.acquire(actx)
	}
	return pool.acquire(ctx)
}

// end runs once: close the rows (if any), finish the ticket, cancel the statement context,
// release the connection. Finish runs before cancel so the guard sees only its own deadline.
func (s *stmt) end(rows pgx.Rows) {
	s.once.Do(func() {
		defer s.conn.Release()
		defer s.cancel()
		defer s.ticket.Finish()
		if rows != nil {
			rows.Close()
		}
	})
}

func (r *PlanRouter) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	s, err := r.begin(ctx, sql, args)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer s.end(nil)
	return s.conn.Exec(s.ctx, sql, s.args...)
}

func (r *PlanRouter) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	s, err := r.begin(ctx, sql, args)
	if err != nil {
		return nil, err
	}
	var rows pgx.Rows
	ok := false
	defer func() {
		if !ok {
			s.end(rows)
		}
	}()
	rows, err = s.conn.Query(s.ctx, sql, s.args...)
	if err != nil {
		return nil, err
	}
	ok = true
	return &routerRows{Rows: rows, s: s}, nil
}

func (r *PlanRouter) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	rows, err := r.Query(ctx, sql, args...)
	return routerRow{rows: rows, err: err}
}

// routerRows gives the connection back when the rows close, when Next reports the end, or when a
// Scan fails, as pgxpool's own rows do. Close stays safe to call again.
type routerRows struct {
	pgx.Rows
	s *stmt
}

func (r *routerRows) Close() { r.s.end(r.Rows) }

func (r *routerRows) Next() bool {
	if r.Rows.Next() {
		return true
	}
	r.s.end(r.Rows)
	return false
}

func (r *routerRows) Scan(dest ...any) error {
	err := r.Rows.Scan(dest...)
	if err != nil {
		r.s.end(r.Rows)
	}
	return err
}

// NewAPIStore builds the store the API reads through: every statement goes through a PlanRouter
// over p's API pools, gated by gate. It has no dedicated pool, so rollups, sweeps and statement
// statistics refuse to run on it; they belong on the maintenance store.
func NewAPIStore(p *Pools, layout planpolicy.Layout, gate guards.Gate, clockDriftThreshold, staleThreshold time.Duration, opts ...Option) *Store {
	return New(NewPlanRouter(p, layout, gate), nil, clockDriftThreshold, staleThreshold, append([]Option{WithPayloadTypeGate()}, opts...)...)
}

// routerRow scans the first row like pgx's own Row: no row is pgx.ErrNoRows, and the rows
// are closed on every path.
type routerRow struct {
	rows pgx.Rows
	err  error
}

func (r routerRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return pgx.ErrNoRows
	}
	if err := r.rows.Scan(dest...); err != nil {
		return err
	}
	r.rows.Close()
	return r.rows.Err()
}
