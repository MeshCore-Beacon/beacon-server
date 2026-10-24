// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/guards"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// fakeCall is one statement a fake connection ran.
type fakeCall struct {
	sql  string
	args []any
}

// fakePool hands out fakeConns and records what ran on them.
type fakePool struct {
	n string

	// Behaviour of its connections.
	acquireErr error
	queryErr   error
	execErr    error
	scanErr    error
	rowCount   int
	block      time.Duration // a statement holds this long unless its ctx ends first

	mu              sync.Mutex
	calls           []fakeCall
	acquires        int
	releases        int
	acquireDeadline bool    // some acquire ctx carried a deadline
	stmtErrs        []error // the statement ctx's Err() when the statement returned
	closeErrs       []error // the statement ctx's Err() when its rows closed
}

func (p *fakePool) name() string { return p.n }

func (p *fakePool) acquire(ctx context.Context) (routerConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := ctx.Deadline(); ok {
		p.acquireDeadline = true
	}
	if p.acquireErr != nil {
		return nil, p.acquireErr
	}
	p.acquires++
	return &fakeConn{p: p}, nil
}

func (p *fakePool) snapshot() (calls []fakeCall, acquires, releases int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]fakeCall(nil), p.calls...), p.acquires, p.releases
}

type fakeConn struct {
	p        *fakePool
	released bool
}

func (c *fakeConn) Release() {
	c.p.mu.Lock()
	defer c.p.mu.Unlock()
	if c.released {
		panic("connection released twice")
	}
	c.released = true
	c.p.releases++
}

// run records the call and holds it for p.block, returning ctx's error if ctx ends first.
func (c *fakeConn) run(ctx context.Context, sql string, args []any) error {
	c.p.mu.Lock()
	c.p.calls = append(c.p.calls, fakeCall{sql: sql, args: args})
	block := c.p.block
	c.p.mu.Unlock()
	var err error
	if block > 0 {
		select {
		case <-time.After(block):
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	c.p.mu.Lock()
	c.p.stmtErrs = append(c.p.stmtErrs, ctx.Err())
	c.p.mu.Unlock()
	return err
}

func (c *fakeConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := c.run(ctx, sql, args); err != nil {
		return pgconn.CommandTag{}, err
	}
	return pgconn.NewCommandTag("SELECT 1"), c.p.execErr
}

func (c *fakeConn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := c.run(ctx, sql, args); err != nil {
		return &fakeRows{p: c.p, ctx: ctx, err: err}, err
	}
	if c.p.queryErr != nil {
		return &fakeRows{p: c.p, ctx: ctx, err: c.p.queryErr}, c.p.queryErr
	}
	return &fakeRows{p: c.p, ctx: ctx, left: c.p.rowCount}, nil
}

func (c *fakeConn) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("the router must build QueryRow on Query")
}

// fakeRows yields left rows of the single int column 7.
type fakeRows struct {
	p      *fakePool
	ctx    context.Context
	left   int
	err    error
	closed bool
}

func (r *fakeRows) Close() {
	if r.closed {
		return
	}
	r.closed = true
	r.p.mu.Lock()
	r.p.closeErrs = append(r.p.closeErrs, r.ctx.Err())
	r.p.mu.Unlock()
}
func (r *fakeRows) Err() error                                   { return r.err }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.NewCommandTag("SELECT") }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Next() bool {
	if r.closed || r.left == 0 {
		r.Close()
		return false
	}
	r.left--
	return true
}
func (r *fakeRows) Scan(dest ...any) error {
	if r.p.scanErr != nil {
		return r.p.scanErr
	}
	*dest[0].(*int) = 7
	return nil
}
func (r *fakeRows) Values() ([]any, error) { return []any{7}, nil }
func (r *fakeRows) RawValues() [][]byte    { return nil }
func (r *fakeRows) Conn() *pgx.Conn        { return nil }

// countingGate counts admissions and checks each ticket's Finish/cancel order.
type countingGate struct {
	mu       sync.Mutex
	admitErr error
	admits   int
	tickets  []*countingTicket
}

func (g *countingGate) Admit(context.Context) (guards.Ticket, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.admits++
	if g.admitErr != nil {
		return nil, g.admitErr
	}
	t := &countingTicket{}
	g.tickets = append(g.tickets, t)
	return t, nil
}

type countingTicket struct {
	mu                    sync.Mutex
	acquired, started     int
	finishes, cancels     int
	cancelledBeforeFinish bool
}

func (t *countingTicket) AcquireTimeout() time.Duration { return 0 }
func (t *countingTicket) Acquired(error) {
	t.mu.Lock()
	t.acquired++
	t.mu.Unlock()
}
func (t *countingTicket) Start(ctx context.Context) (context.Context, context.CancelFunc) {
	t.mu.Lock()
	t.started++
	t.mu.Unlock()
	sctx, cancel := context.WithCancel(ctx)
	return sctx, func() {
		t.mu.Lock()
		t.cancels++
		t.mu.Unlock()
		cancel()
	}
}
func (t *countingTicket) Finish() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cancels > 0 {
		t.cancelledBeforeFinish = true
	}
	t.finishes++
}

type routerFixture struct {
	legacy, slow, custom, cached *fakePool
	gate                         *countingGate
	r                            *PlanRouter
}

func newRouterFixture(layout planpolicy.Layout, classOf func(string) planpolicy.Class) *routerFixture {
	f := &routerFixture{
		legacy: &fakePool{n: PoolAPILegacy, rowCount: 1},
		slow:   &fakePool{n: PoolAPISlow, rowCount: 1},
		custom: &fakePool{n: PoolAPICustom, rowCount: 1},
		cached: &fakePool{n: PoolAPICached, rowCount: 1},
		gate:   &countingGate{},
	}
	f.r = newPlanRouter(f.legacy, f.slow, f.custom, f.cached, layout, f.gate, classOf)
	return f
}

func (f *routerFixture) pool(name string) *fakePool {
	return map[string]*fakePool{PoolAPILegacy: f.legacy, PoolAPISlow: f.slow, PoolAPICustom: f.custom, PoolAPICached: f.cached}[name]
}

var routerMethods = []string{"Query", "QueryRow", "Exec"}

// runVia runs sql once through method and returns its error.
func runVia(ctx context.Context, r *PlanRouter, method, sql string, args ...any) error {
	switch method {
	case "Query":
		rows, err := r.Query(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var n int
			if err := rows.Scan(&n); err != nil {
				return err
			}
		}
		rows.Close()
		return rows.Err()
	case "QueryRow":
		var n int
		return r.QueryRow(ctx, sql, args...).Scan(&n)
	default:
		_, err := r.Exec(ctx, sql, args...)
		return err
	}
}

func TestPlanRouterRoutesByClass(t *testing.T) {
	classOf := func(name string) planpolicy.Class {
		return map[string]planpolicy.Class{"ListPacketsByIATAs": planpolicy.ClassSlow, "ProbeG": planpolicy.ClassG}[name] // else ClassC
	}
	for _, c := range []struct {
		layout     planpolicy.Layout
		stmt, pool string
		describe   bool
		admits     int
	}{
		{planpolicy.CustomAll, "ListPacketsByIATAs", PoolAPISlow, true, 0},
		{planpolicy.CustomAll, "ProbeG", PoolAPICustom, false, 1},
		{planpolicy.CustomAll, "ProbeC", PoolAPICustom, true, 1},
		{planpolicy.Split, "ListPacketsByIATAs", PoolAPISlow, true, 0},
		{planpolicy.Split, "ProbeG", PoolAPICached, false, 1},
		{planpolicy.Split, "ProbeC", PoolAPICustom, true, 1},
		{planpolicy.Single, "ListPacketsByIATAs", PoolAPILegacy, true, 0},
		{planpolicy.Single, "ProbeG", PoolAPILegacy, false, 1},
		{planpolicy.Single, "ProbeC", PoolAPILegacy, true, 1},
	} {
		for _, method := range routerMethods {
			t.Run(fmt.Sprintf("%s/%s/%s", c.layout, c.stmt, method), func(t *testing.T) {
				f := newRouterFixture(c.layout, classOf)
				sql := "-- name: " + c.stmt + " :many\nSELECT $1"
				if err := runVia(context.Background(), f.r, method, sql, 7); err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{PoolAPILegacy, PoolAPISlow, PoolAPICustom, PoolAPICached} {
					calls, _, _ := f.pool(name).snapshot()
					if name != c.pool {
						if len(calls) != 0 {
							t.Errorf("%s ran %d statements, want 0", name, len(calls))
						}
						continue
					}
					if len(calls) != 1 || calls[0].sql != sql {
						t.Fatalf("%s calls = %+v, want one %q", name, calls, sql)
					}
					want := []any{7}
					if c.describe {
						want = []any{pgx.QueryExecModeCacheDescribe, 7}
					}
					if !reflect.DeepEqual(calls[0].args, want) {
						t.Errorf("args = %v, want %v", calls[0].args, want)
					}
				}
				if f.gate.admits != c.admits {
					t.Errorf("admits = %d, want %d", f.gate.admits, c.admits)
				}
			})
		}
	}

	// A class C statement never reaches api_cached, and nothing does outside split.
	for _, layout := range []planpolicy.Layout{planpolicy.CustomAll, planpolicy.Split, planpolicy.Single} {
		f := newRouterFixture(layout, nil) // the real policy
		for _, stmt := range []string{"ListPacketsByIATAs", "ListChannels", "ListKnownRoutesByIATAHop", "ListKnownRoutesGlobalHop", ""} {
			sql := "SELECT 1"
			if stmt != "" {
				sql = "-- name: " + stmt + " :one\nSELECT 1"
			}
			for _, method := range routerMethods {
				if err := runVia(context.Background(), f.r, method, sql, 7); err != nil {
					t.Fatal(err)
				}
			}
		}
		if calls, _, _ := f.cached.snapshot(); len(calls) != 0 {
			t.Errorf("%s: api_cached ran %d statements, want 0", layout, len(calls))
		}
		// The hop pair always runs unnamed, with its own args after the mode.
		hopPool := f.custom
		if layout == planpolicy.Single {
			hopPool = f.legacy
		}
		calls, _, _ := hopPool.snapshot()
		hops := 0
		for _, c := range calls {
			if !strings.Contains(c.sql, "Hop :one") {
				continue
			}
			hops++
			if want := []any{pgx.QueryExecModeCacheDescribe, 7}; !reflect.DeepEqual(c.args, want) {
				t.Errorf("%s: %q args = %v, want %v", layout, c.sql, c.args, want)
			}
		}
		if hops != 2*len(routerMethods) {
			t.Errorf("%s: %d hop statements on %s, want %d", layout, hops, hopPool.n, 2*len(routerMethods))
		}
	}

	// The real policy admits GetNodeByID (class G): under split it runs named on api_cached, with no mode argument.
	f := newRouterFixture(planpolicy.Split, nil)
	sql := "-- name: GetNodeByID :one\nSELECT $1"
	for _, method := range routerMethods {
		if err := runVia(context.Background(), f.r, method, sql, 7); err != nil {
			t.Fatal(err)
		}
	}
	calls, _, _ := f.cached.snapshot()
	if len(calls) != len(routerMethods) {
		t.Fatalf("split: api_cached ran %d GetNodeByID statements, want %d", len(calls), len(routerMethods))
	}
	for _, c := range calls {
		if c.sql != sql || !reflect.DeepEqual(c.args, []any{7}) {
			t.Errorf("split: api_cached call %q %v, want %q [7]", c.sql, c.args, sql)
		}
	}
	for _, p := range []*fakePool{f.custom, f.legacy} {
		if calls, _, _ := p.snapshot(); len(calls) != 0 {
			t.Errorf("split: %s ran %d statements, want 0", p.n, len(calls))
		}
	}

	// Through NewPlanRouter with real, unconnected pools: no layout dereferences a pool it did not open.
	cfg, err := pgxpool.ParseConfig("postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	unconnected := func() *pgxpool.Pool {
		p, err := pgxpool.NewWithConfig(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}
	for _, tc := range []struct {
		layout planpolicy.Layout
		pools  *Pools
	}{
		{planpolicy.CustomAll, &Pools{APISlow: unconnected(), APICustom: unconnected()}},
		{planpolicy.Single, &Pools{APILegacy: unconnected()}},
	} {
		r := NewPlanRouter(tc.pools, tc.layout, nil)
		for _, stmt := range []string{"ListPacketsByIATAs", "GetNodeByID"} {
			for _, method := range routerMethods {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				err := runVia(ctx, r, method, "-- name: "+stmt+" :one\nSELECT 1")
				cancel()
				if err == nil {
					t.Errorf("%s %s %s: no error from an unreachable server", tc.layout, stmt, method)
				}
			}
		}
	}
}

func TestPlanRouterReleasesConnections(t *testing.T) {
	const sql = "-- name: ProbeC :one\nSELECT 1"
	errQuery, errScan, errExec, errAcquire, errAdmit := errors.New("query"), errors.New("scan"), errors.New("exec"), errors.New("acquire"), errors.New("admit")
	for _, c := range []struct {
		name               string
		setup              func(p *fakePool, g *countingGate)
		run                func(r *PlanRouter) error
		wantErr            error
		acquires, finishes int
	}{
		{"query read to the end", func(p *fakePool, _ *countingGate) { p.rowCount = 3 }, func(r *PlanRouter) error {
			return runVia(context.Background(), r, "Query", sql)
		}, nil, 1, 1},
		{"query closed early", func(p *fakePool, _ *countingGate) { p.rowCount = 3 }, func(r *PlanRouter) error {
			rows, err := r.Query(context.Background(), sql)
			if err != nil {
				return err
			}
			rows.Next()
			rows.Close()
			return nil
		}, nil, 1, 1},
		{"query error", func(p *fakePool, _ *countingGate) { p.queryErr = errQuery }, func(r *PlanRouter) error {
			_, err := r.Query(context.Background(), sql)
			return err
		}, errQuery, 1, 1},
		{"queryrow no rows", func(p *fakePool, _ *countingGate) { p.rowCount = 0 }, func(r *PlanRouter) error {
			return runVia(context.Background(), r, "QueryRow", sql)
		}, pgx.ErrNoRows, 1, 1},
		{"queryrow query error", func(p *fakePool, _ *countingGate) { p.queryErr = errQuery }, func(r *PlanRouter) error {
			return runVia(context.Background(), r, "QueryRow", sql)
		}, errQuery, 1, 1},
		{"queryrow scan error", func(p *fakePool, _ *countingGate) { p.scanErr = errScan }, func(r *PlanRouter) error {
			return runVia(context.Background(), r, "QueryRow", sql)
		}, errScan, 1, 1},
		{"exec", func(*fakePool, *countingGate) {}, func(r *PlanRouter) error {
			return runVia(context.Background(), r, "Exec", sql)
		}, nil, 1, 1},
		{"exec error", func(p *fakePool, _ *countingGate) { p.execErr = errExec }, func(r *PlanRouter) error {
			return runVia(context.Background(), r, "Exec", sql)
		}, errExec, 1, 1},
		{"query read to the end without Close", func(p *fakePool, _ *countingGate) { p.rowCount = 3 }, func(r *PlanRouter) error {
			rows, err := r.Query(context.Background(), sql)
			if err != nil {
				return err
			}
			for rows.Next() {
			}
			return rows.Err()
		}, nil, 1, 1},
		{"query scan error without Close", func(p *fakePool, _ *countingGate) { p.rowCount = 3; p.scanErr = errScan }, func(r *PlanRouter) error {
			rows, err := r.Query(context.Background(), sql)
			if err != nil {
				return err
			}
			rows.Next()
			var n int
			return rows.Scan(&n)
		}, errScan, 1, 1},
		{"close after the end", func(p *fakePool, _ *countingGate) { p.rowCount = 2 }, func(r *PlanRouter) error {
			rows, err := r.Query(context.Background(), sql)
			if err != nil {
				return err
			}
			for rows.Next() {
			}
			rows.Close()
			rows.Close()
			return rows.Err()
		}, nil, 1, 1},
		{"close twice", func(*fakePool, *countingGate) {}, func(r *PlanRouter) error {
			rows, err := r.Query(context.Background(), sql)
			if err != nil {
				return err
			}
			rows.Close()
			rows.Close()
			return nil
		}, nil, 1, 1},
		{"acquire error", func(p *fakePool, _ *countingGate) { p.acquireErr = errAcquire }, func(r *PlanRouter) error {
			return runVia(context.Background(), r, "Query", sql)
		}, errAcquire, 0, 1},
		{"admit error", func(_ *fakePool, g *countingGate) { g.admitErr = errAdmit }, func(r *PlanRouter) error {
			return runVia(context.Background(), r, "Exec", sql)
		}, errAdmit, 0, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newRouterFixture(planpolicy.CustomAll, nil)
			c.setup(f.custom, f.gate)
			if err := c.run(f.r); !errors.Is(err, c.wantErr) {
				t.Fatalf("err = %v, want %v", err, c.wantErr)
			}
			_, acquires, releases := f.custom.snapshot()
			if acquires != c.acquires || releases != acquires {
				t.Errorf("acquires = %d, releases = %d, want %d each", acquires, releases, c.acquires)
			}
			finishes := 0
			for _, tk := range f.gate.tickets {
				finishes += tk.finishes
				if tk.cancels != tk.started || tk.cancels > 1 {
					t.Errorf("Start = %d, cancel = %d; want one cancel per Start", tk.started, tk.cancels)
				}
				if tk.cancelledBeforeFinish {
					t.Error("cancel ran before Finish")
				}
			}
			if finishes != c.finishes {
				t.Errorf("Finish = %d, want %d", finishes, c.finishes)
			}
			for _, err := range f.custom.closeErrs {
				if err != nil {
					t.Errorf("statement context ended before rows.Close: %v", err)
				}
			}
		})
	}
}

// A slow-pool statement is never cut by a guard deadline: only api_slow's statement limit bounds it.
func TestSlowStatementNeverCut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := guards.New(guards.Options{Enforce: guards.Families, Budget: "shadow"})
		ctx, finish := g.Begin(context.Background(), "k", guards.PacketDetail, guards.Entity) // 5 s
		defer finish()
		slow := &fakePool{n: PoolAPISlow, rowCount: 1, block: 8 * time.Second}
		custom := &fakePool{n: PoolAPICustom, rowCount: 1, block: 8 * time.Second}
		r := newPlanRouter(nil, slow, custom, nil, planpolicy.CustomAll, g, nil)

		start := time.Now()
		if err := runVia(ctx, r, "Query", "-- name: ListPacketsByIATAs :many\nSELECT 1"); err != nil {
			t.Fatalf("slow statement: %v", err)
		}
		if d := time.Since(start); d != 8*time.Second {
			t.Errorf("slow statement took %v, want 8s", d)
		}
		if slow.stmtErrs[0] != nil {
			t.Errorf("slow statement ctx.Err() = %v, want nil", slow.stmtErrs[0])
		}

		start = time.Now()
		err := runVia(ctx, r, "Query", "-- name: ProbeC :many\nSELECT 1")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("class C statement err = %v, want context.DeadlineExceeded", err)
		}
		if d := time.Since(start); d != 5*time.Second {
			t.Errorf("class C statement cut after %v, want 5s", d)
		}
		if _, acquires, releases := custom.snapshot(); acquires != 1 || releases != 1 {
			t.Errorf("custom acquires = %d, releases = %d", acquires, releases)
		}
	})
}

// guardSeries is every beacon_guard_ series line in the metrics scrape.
func guardSeries(t *testing.T) string {
	var lines []string
	for _, l := range strings.Split(scrapeMetrics(t), "\n") {
		if strings.HasPrefix(l, "beacon_guard_") {
			lines = append(lines, l)
		}
	}
	return strings.Join(lines, "\n")
}

func TestPlanRouterWithoutRequestState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := guards.New(guards.Options{Enforce: guards.Families, Budget: "shadow"})
		custom := &fakePool{n: PoolAPICustom, rowCount: 1, block: 40 * time.Second}
		r := newPlanRouter(nil, &fakePool{n: PoolAPISlow}, custom, nil, planpolicy.CustomAll, g, nil)
		before := guardSeries(t)
		start := time.Now()
		for _, method := range routerMethods {
			if err := runVia(context.Background(), r, method, "-- name: ProbeC :one\nSELECT 1"); err != nil {
				t.Fatalf("%s: %v", method, err)
			}
		}
		if d := time.Since(start); d != 120*time.Second {
			t.Errorf("statements took %v, want 3 x 40s", d)
		}
		if after := guardSeries(t); after != before {
			t.Errorf("guard metrics moved:\nbefore:\n%s\nafter:\n%s", before, after)
		}
		if custom.acquireDeadline {
			t.Error("acquire carried a deadline")
		}
		for _, err := range custom.stmtErrs {
			if err != nil {
				t.Errorf("statement ctx.Err() = %v", err)
			}
		}
	})
}

// cappedPool hands out at most cap connections at once; a further acquire waits for one or for its ctx.
type cappedPool struct {
	*fakePool
	slots chan struct{}
}

func newCappedPool(name string, n int, block time.Duration) *cappedPool {
	return &cappedPool{fakePool: &fakePool{n: name, rowCount: 1, block: block}, slots: make(chan struct{}, n)}
}

func (p *cappedPool) acquire(ctx context.Context) (routerConn, error) {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	c, err := p.fakePool.acquire(ctx)
	if err != nil {
		<-p.slots
		return nil, err
	}
	return &cappedConn{routerConn: c, free: func() { <-p.slots }}, nil
}

type cappedConn struct {
	routerConn
	free func()
}

func (c *cappedConn) Release() { c.routerConn.Release(); c.free() }

// Two slow statements hold both connections of a two-connection api_slow: a third fails fast with ErrSlowPoolBusy, never touching
// api_custom, while class C statements keep running on api_custom; once a slot frees, slow statements run again.
func TestPlanRouterSlowPoolCannotStarveOthers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		old := slowAcquireWait
		slowAcquireWait = 5 * time.Second
		t.Cleanup(func() { slowAcquireWait = old })
		slow := newCappedPool(PoolAPISlow, 2, 100*time.Second)
		custom := &fakePool{n: PoolAPICustom, rowCount: 1}
		r := newPlanRouter(nil, slow, custom, nil, planpolicy.CustomAll, nil, nil)
		const slowSQL = "-- name: ListPacketsByIATAs :many\nSELECT 1"

		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() {
				if err := runVia(context.Background(), r, "Query", slowSQL); err != nil {
					t.Errorf("holding statement: %v", err)
				}
			})
		}
		synctest.Wait()

		start := time.Now()
		err := runVia(context.Background(), r, "Query", slowSQL)
		if !errors.Is(err, api.ErrSlowPoolBusy) {
			t.Fatalf("third slow statement err = %v, want ErrSlowPoolBusy", err)
		}
		if d := time.Since(start); d != 5*time.Second {
			t.Errorf("third slow statement failed after %v, want 5s", d)
		}
		// A request that ends first gets its own error, not ErrSlowPoolBusy.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err = runVia(ctx, r, "Query", slowSQL)
		cancel()
		if errors.Is(err, api.ErrSlowPoolBusy) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("slow statement under a shorter request deadline: err = %v", err)
		}
		for range 3 {
			if err := runVia(context.Background(), r, "Query", "-- name: ProbeC :many\nSELECT 1"); err != nil {
				t.Fatalf("class C statement while api_slow is full: %v", err)
			}
		}
		if calls, acquires, _ := custom.snapshot(); acquires != 3 || len(calls) != 3 {
			t.Errorf("api_custom ran %d statements in %d acquires, want 3 class C statements only", len(calls), acquires)
		}
		wg.Wait()
		if err := runVia(context.Background(), r, "Query", slowSQL); err != nil {
			t.Errorf("slow statement after the pool freed: %v", err)
		}
		if _, acquires, releases := slow.snapshot(); acquires != 3 || releases != 3 {
			t.Errorf("api_slow acquires = %d, releases = %d, want 3 and 3", acquires, releases)
		}
	})
}

// A verification-only statement never reaches an API pool: the router refuses it before admission or acquire.
func TestPlanRouterRefusesVerificationOnly(t *testing.T) {
	for _, layout := range []planpolicy.Layout{planpolicy.CustomAll, planpolicy.Split, planpolicy.Single} {
		f := newRouterFixture(layout, nil)
		for _, method := range routerMethods {
			err := runVia(context.Background(), f.r, method, "-- name: ListChannelMessages :many\nSELECT 1")
			if err == nil || !strings.Contains(err.Error(), "verification") {
				t.Errorf("%s %s: err = %v, want a refusal", layout, method, err)
			}
		}
		for _, p := range []*fakePool{f.legacy, f.slow, f.custom, f.cached} {
			if _, acquires, _ := p.snapshot(); acquires != 0 {
				t.Errorf("%s: %s acquired %d connections", layout, p.n, acquires)
			}
		}
		if f.gate.admits != 0 {
			t.Errorf("%s: gate admitted %d", layout, f.gate.admits)
		}
	}
}

// A planpolicy.ParallelWorkers statement runs on its API pool inside its own transaction that raises the worker
// setting, and the transaction ends before the connection goes back; every other statement, and the rollback
// layout's api_legacy (the server's own settings), runs bare.
func TestPlanRouterParallelWorkers(t *testing.T) {
	const begin = "BEGIN READ ONLY; SET LOCAL max_parallel_workers_per_gather = 2"
	for _, layout := range []planpolicy.Layout{planpolicy.CustomAll, planpolicy.Split, planpolicy.Single} {
		for _, stmt := range []string{"GetTopNodesAll", "GetTopNodesProbe"} {
			for _, method := range routerMethods {
				t.Run(fmt.Sprintf("%s/%s/%s", layout, stmt, method), func(t *testing.T) {
					f := newRouterFixture(layout, nil) // the real policy
					sql := "-- name: " + stmt + " :many\nSELECT $1"
					if err := runVia(context.Background(), f.r, method, sql, 7); err != nil {
						t.Fatal(err)
					}
					pool := PoolAPICustom
					if layout == planpolicy.Single {
						pool = PoolAPILegacy
					}
					calls, acquires, releases := f.pool(pool).snapshot()
					var got []string
					for _, c := range calls {
						got = append(got, c.sql)
					}
					want := []string{sql}
					if stmt == "GetTopNodesAll" && layout != planpolicy.Single {
						want = []string{begin, sql, "ROLLBACK"}
					}
					if !reflect.DeepEqual(got, want) {
						t.Errorf("%s ran %q, want %q", pool, got, want)
					}
					if acquires != 1 || releases != 1 {
						t.Errorf("acquires %d releases %d, want 1 and 1", acquires, releases)
					}
				})
			}
		}
	}

	// BEGIN fails: the statement does not run, its error comes back, and the ticket and connection are given back once.
	f := newRouterFixture(planpolicy.CustomAll, nil)
	f.custom.execErr = errors.New("begin failed")
	sql := "-- name: GetTopNodesAll :many\nSELECT $1"
	if err := runVia(context.Background(), f.r, "Query", sql, 7); err == nil || err.Error() != "begin failed" {
		t.Fatalf("err = %v, want begin failed", err)
	}
	calls, acquires, releases := f.custom.snapshot()
	if len(calls) != 2 || calls[0].sql != begin || calls[1].sql != "ROLLBACK" || acquires != 1 || releases != 1 {
		t.Errorf("calls %+v acquires %d releases %d, want BEGIN, ROLLBACK, 1, 1", calls, acquires, releases)
	}
	if len(f.gate.tickets) != 1 || f.gate.tickets[0].finishes != 1 {
		t.Errorf("tickets %d, want one finished once", len(f.gate.tickets))
	}
}
