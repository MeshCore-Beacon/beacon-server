// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fillSliceSQL is FillPacketSitesSlice's statement with positional parameters, read from its query file.
func fillSliceSQL(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("queries/packet_sites.sql")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	i := strings.Index(s, "-- name: FillPacketSitesSlice ")
	if i < 0 {
		t.Fatal("FillPacketSitesSlice not found")
	}
	s = s[i:]
	s = s[:strings.Index(s, ";")]
	var lines []string
	for _, l := range strings.Split(s, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "--") {
			lines = append(lines, l)
		}
	}
	s = strings.Join(lines, "\n")
	return strings.NewReplacer("@after_id::bigint", "$1::bigint", "@through_id::bigint", "$2::bigint").Replace(s)
}

// explainFill prepares the fill statement on conn and returns EXPLAIN EXECUTE's plan for (after, through].
func explainFill(t *testing.T, ctx context.Context, conn *pgxpool.Conn, after, through int64) string {
	t.Helper()
	if _, err := conn.Exec(ctx, "PREPARE fill_plan_check(bigint, bigint) AS "+fillSliceSQL(t)); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(context.Background(), "DEALLOCATE fill_plan_check") //nolint:errcheck // test cleanup
	rows, err := conn.Query(ctx, fmt.Sprintf("EXPLAIN EXECUTE fill_plan_check(%d, %d)", after, through))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, line)
	}
	return strings.Join(plan, "\n")
}

func showPlanMode(t *testing.T, ctx context.Context, conn *pgxpool.Conn) string {
	t.Helper()
	var mode string
	if err := conn.QueryRow(ctx, "SHOW plan_cache_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	return mode
}

func reltuples(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) float64 {
	t.Helper()
	var n float64
	if err := pool.QueryRow(ctx, "SELECT reltuples FROM pg_class WHERE oid = $1::regclass", table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestFillSessionsPlanCustomPostgres guards against the fill stall: with automatic plan caching, the prepared fill
// statement turned generic after five slices and, planned while the new packet_sites table had no statistics, the
// 6th slice of a 3-million-observation backfill ran past a 2-minute timeout instead of taking 14 s. A pool that
// forces generic plans stands for that old path: its connections plan the fill without the slice's bounds. The
// fill sessions on the same pool plan it with them, ANALYZE a table that has no statistics when they start and
// again after their first slice or batch, and leave the pool's own setting behind when they close.
func TestFillSessionsPlanCustomPostgres(t *testing.T) {
	f := newPsFixture(t)
	cfg := f.pool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["plan_cache_mode"] = "force_generic_plan"
	cfg.MaxConns = 1 // one connection, so the checks below see the session's own
	generic, err := pgxpool.NewWithConfig(f.ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(generic.Close)
	f.unsited(f.packets(30)...)
	hi := f.maxID()
	if n := reltuples(t, f.ctx, f.pool, "packet_sites"); n >= 0 {
		t.Fatalf("a new packet_sites has reltuples %v, want -1 (never analyzed)", n)
	}

	// The old path: a generic plan, blind to the slice's bounds.
	func() {
		conn, err := generic.Acquire(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		if plan := explainFill(t, f.ctx, conn, hi-50, hi); !strings.Contains(plan, "$1") {
			t.Fatalf("the pool's own plan is not generic:\n%s", plan)
		}
	}()

	// The fill session: custom plans with the bounds as constants, and statistics from the start.
	store := New(generic, generic, 0, 0, WithFillPacing(FillPacing{Duty: 1, PacketSitesSlice: 50}))
	p, ok, err := store.BeginPacketSites(f.ctx)
	if err != nil || !ok {
		t.Fatalf("BeginPacketSites = %v, %v", ok, err)
	}
	t.Cleanup(func() { p.Close(context.Background()) })
	if mode := showPlanMode(t, f.ctx, p.conn); mode != "force_custom_plan" {
		t.Fatalf("fill session plan_cache_mode %s", mode)
	}
	if n := reltuples(t, f.ctx, f.pool, "packet_sites"); n < 0 {
		t.Fatal("packet_sites still has no statistics after the session started")
	}
	plan := explainFill(t, f.ctx, p.conn, hi-50, hi)
	if strings.Contains(plan, "$1") || !strings.Contains(plan, "'"+strconv.FormatInt(hi-50, 10)+"'::bigint") {
		t.Fatalf("the fill session's plan is not custom:\n%s", plan)
	}
	if _, err := p.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if st, err := p.Backfill(f.ctx, time.Minute); err != nil || !st.Complete {
		t.Fatalf("Backfill = %+v, %v", st, err)
	}
	if !p.analyzed {
		t.Fatal("no ANALYZE after the first slice")
	}
	if n := reltuples(t, f.ctx, f.pool, "packet_sites"); n <= 0 {
		t.Fatalf("packet_sites reltuples %v after the backfill's ANALYZE", n)
	}
	t.Logf("backfill of %d slices took %v", len(p.fills), time.Since(start))
	p.Close(f.ctx)
	f.wantClean("after the backfill on the generic pool")
	func() {
		conn, err := generic.Acquire(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		if mode := showPlanMode(t, f.ctx, conn); mode != "force_generic_plan" {
			t.Fatalf("after Close the connection has plan_cache_mode %s, want the pool's force_generic_plan", mode)
		}
	}()

	// cmi: the same session setting, and statistics for a new channel_message_iatas.
	c := newCmiFixture(t)
	for i := range 5 {
		h := c.packet(string(rune('a'+i)), 3)
		c.obs(c.pool, h, "YUL", true)
		c.unseeded(cmiBase+int64(1+i), 7, h)
	}
	ccfg := c.pool.Config().Copy()
	ccfg.ConnConfig.RuntimeParams["plan_cache_mode"] = "force_generic_plan"
	ccfg.MaxConns = 1
	cgeneric, err := pgxpool.NewWithConfig(c.ctx, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cgeneric.Close)
	if n := reltuples(t, c.ctx, c.pool, "channel_message_iatas"); n >= 0 {
		t.Fatalf("a new channel_message_iatas has reltuples %v", n)
	}
	cs, ok, err := New(cgeneric, cgeneric, 0, 0, WithFillPacing(FillPacing{Duty: 1, CmiBatch: 2})).BeginCmi(c.ctx)
	if err != nil || !ok {
		t.Fatalf("BeginCmi = %v, %v", ok, err)
	}
	t.Cleanup(func() { cs.Close(context.Background()) })
	if mode := showPlanMode(t, c.ctx, cs.conn); mode != "force_custom_plan" {
		t.Fatalf("cmi session plan_cache_mode %s", mode)
	}
	if n := reltuples(t, c.ctx, c.pool, "channel_message_iatas"); n < 0 {
		t.Fatal("channel_message_iatas still has no statistics after the session started")
	}
	if _, err := cs.Init(c.ctx); err != nil {
		t.Fatal(err)
	}
	if st, err := cs.Backfill(c.ctx, time.Minute); err != nil || !st.Complete || !cs.analyzed {
		t.Fatalf("Backfill = %+v, %v, analyzed %v", st, err, cs.analyzed)
	}
	cs.Close(c.ctx)
	if got := c.check(); got.Missing != 0 || got.Extra != 0 {
		t.Fatalf("cmi check = %+v", got)
	}

	// The payload-type backfill's statement runs on the generic pool too, and still types its rows.
	if _, _, err := store.BackfillObservationPayloadTypes(f.ctx, 0, hi+1, 1000); err != nil {
		t.Fatal(err)
	}
}

// TestFillAnalyzeFailureIsIgnoredPostgres checks that a failed ANALYZE (here: a lock it cannot get in time) is
// logged and the fill goes on.
func TestFillAnalyzeFailureIsIgnoredPostgres(t *testing.T) {
	f := newPsFixture(t)
	f.unsited(f.packets(10)...)
	defer func(old string) { fillAnalyzeLockTimeout = old }(fillAnalyzeLockTimeout)
	fillAnalyzeLockTimeout = "200ms"
	holder, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(context.Background()) //nolint:errcheck // test
	// SHARE UPDATE EXCLUSIVE conflicts with ANALYZE's own lock and not with the fill's writes.
	if _, err := holder.Exec(f.ctx, "LOCK TABLE packet_sites IN SHARE UPDATE EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	p := f.begin() // the session-start ANALYZE times out on the lock
	if _, err := p.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	if st, err := p.Backfill(f.ctx, time.Minute); err != nil || !st.Complete {
		t.Fatalf("Backfill with ANALYZE blocked = %+v, %v", st, err)
	}
	if err := holder.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.wantClean("after a backfill whose ANALYZE failed")
}
