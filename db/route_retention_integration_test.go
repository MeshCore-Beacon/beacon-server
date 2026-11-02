// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// legacyDeleteOldRoutes is the pre-change statement, DeleteOldRoutes as sqlc generated it: the deleted set the new
// statements must reproduce.
const legacyDeleteOldRoutes = `WITH expired AS (
    SELECT r.iata, r.path_key
    FROM known_routes r
    WHERE r.last_seen < GREATEST($1::timestamptz, $2::timestamptz)
      AND (r.last_seen < $1 OR
           (r.observation_count < $3 AND r.last_seen < $2))
    LIMIT $4
    FOR UPDATE OF r SKIP LOCKED
)
DELETE FROM known_routes kr USING expired e
WHERE kr.iata = e.iata AND kr.path_key = e.path_key`

func floorMs(t time.Time) time.Time { return t.Truncate(time.Millisecond) }

// retentionFixture fills known_routes around the cutoffs R and G (either order), with rows on
// both sides of each at µs and ms granularity and a µs tie group inside one millisecond.
func retentionFixture(t *testing.T, ctx context.Context, tx pgx.Tx, R, G time.Time, m int64) {
	t.Helper()
	lo, hi := R, G
	if hi.Before(lo) {
		lo, hi = hi, lo
	}
	var ts []time.Time
	var obs []int64
	add := func(at time.Time, o int64) { ts, obs = append(ts, at), append(obs, o) }
	for i := 1; i <= 30; i++ {
		add(lo.Add(-time.Duration(i)*37*time.Second), int64(1+i%5))
	}
	for _, c := range []time.Time{R, G} {
		for _, at := range []time.Time{c.Add(-time.Microsecond), floorMs(c), c, c.Add(time.Microsecond), ceilMs(c)} {
			for _, o := range []int64{m - 1, m, m + 1} {
				add(at, o)
			}
		}
	}
	for i := 0; i < 25; i++ {
		add(floorMs(G).Add(-3*time.Millisecond+time.Duration(i)*time.Microsecond), 1)
	}
	if span := hi.Sub(lo); span > 0 {
		for i := 1; i <= 40; i++ {
			add(lo.Add(span*time.Duration(i)/41), 1+int64(i)%(m+1))
		}
	}
	for i := 1; i <= 10; i++ {
		add(hi.Add(time.Duration(i)*time.Hour), 1)
	}
	insertRoutes(t, ctx, tx, ts, obs)
}

// insertRoutes appends rows to known_routes, ids continuing after the current maximum.
func insertRoutes(t *testing.T, ctx context.Context, tx pgx.Tx, ts []time.Time, obs []int64) {
	t.Helper()
	_, err := tx.Exec(ctx, `
INSERT INTO known_routes (id, path_key, node_ids, hash_prefix, iata, hop_count, last_seen, observation_count)
SELECT b.base + u.n, int8send(b.base + u.n), ARRAY[md5((b.base + u.n)::text)::uuid], ARRAY['\x01'::bytea], 'YYZ', 1, u.ts, u.obs
FROM (SELECT coalesce(max(id), 0) AS base FROM known_routes) b,
     unnest($1::timestamptz[], $2::bigint[]) WITH ORDINALITY AS u(ts, obs, n)`, ts, obs)
	if err != nil {
		t.Fatal(err)
	}
}

// legacyLoop runs today's statement to completion on legacy_kr, a copy of known_routes taken on
// the first call.
func legacyLoop(t *testing.T, ctx context.Context, tx pgx.Tx, R, G time.Time, m int64) {
	t.Helper()
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('legacy_kr') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		mustExec(t, ctx, tx, `CREATE TABLE legacy_kr (LIKE known_routes INCLUDING ALL); INSERT INTO legacy_kr SELECT * FROM known_routes`)
	}
	sql := strings.ReplaceAll(legacyDeleteOldRoutes, "known_routes", "legacy_kr")
	for {
		tag, err := tx.Exec(ctx, sql, pgtype.Timestamptz{Time: R, Valid: true}, pgtype.Timestamptz{Time: G, Valid: true}, m, int64(10_000))
		if err != nil {
			t.Fatal(err)
		}
		if tag.RowsAffected() < 10_000 {
			return
		}
	}
}

func keyLess(a, b RouteKey) bool {
	return a.TS.Before(b.TS) || (a.TS.Equal(b.TS) && a.ID < b.ID)
}

// deleteExpiredAll is one retention run: DeleteExpiredRoutes from the start until a short batch.
func deleteExpiredAll(t *testing.T, ctx context.Context, s *Store, R time.Time, batch int32) int64 {
	t.Helper()
	var after RouteKey
	var total int64
	for calls := 0; ; calls++ {
		if calls > 10_000 {
			t.Fatal("DeleteExpiredRoutes does not terminate")
		}
		selected, last, err := s.DeleteExpiredRoutes(ctx, after, R, batch)
		if err != nil {
			t.Fatal(err)
		}
		total += selected
		if selected < int64(batch) {
			return total
		}
		if !keyLess(after, last) {
			t.Fatalf("cursor did not advance: %v -> %v", after, last)
		}
		after = last
	}
}

// graceWalk is one grace run as RouteRetentionTask drives it: windows from start up to G. It returns the
// watermark the next run starts from, min(floor_ms(G), earliest unresolved candidate).
func graceWalk(t *testing.T, ctx context.Context, s *Store, start, G time.Time, m int64, batch int32) time.Time {
	t.Helper()
	wm := floorMs(G)
	after := RouteKey{TS: start}
	for calls := 0; ; calls++ {
		if calls > 10_000 {
			t.Fatal("grace walk does not terminate")
		}
		res, err := s.DeleteGraceRoutesSlice(ctx, after, G, m, batch)
		if err != nil {
			t.Fatal(err)
		}
		if res.Unresolved != nil && res.Unresolved.Before(wm) {
			wm = *res.Unresolved
		}
		switch {
		case res.Candidates == int64(batch):
			if !keyLess(after, res.Last) {
				t.Fatalf("cursor did not advance: %v -> %v", after, res.Last)
			}
			after = res.Last
		case res.WindowEnd.Before(G):
			after = RouteKey{TS: res.WindowEnd}
		default:
			return wm
		}
	}
}

// assertSameKeys fails unless the two tables hold the same route ids.
func assertSameKeys(t *testing.T, ctx context.Context, tx pgx.Tx, a, b string) {
	t.Helper()
	q := func(x, y string) []int64 {
		return queryInt64s(t, ctx, tx, `SELECT id FROM `+x+` EXCEPT SELECT id FROM `+y+` ORDER BY 1`)
	}
	if extra := q(a, b); len(extra) > 0 {
		t.Errorf("%s keeps ids %s deleted: %v", a, b, extra)
	}
	if missing := q(b, a); len(missing) > 0 {
		t.Errorf("%s deleted ids %s keeps: %v", a, b, missing)
	}
}

func countAt(t *testing.T, ctx context.Context, tx pgx.Tx, at time.Time, obs int64) int {
	t.Helper()
	var n int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM known_routes WHERE last_seen = $1 AND observation_count = $2`,
		pgtype.Timestamptz{Time: at, Valid: true}, obs).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Exact cutoffs: R and G carry µs (…00.000586), as today's now.Add(-d) does.
func TestRouteRetentionCutoffsExactPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	R := time.Date(2026, 1, 1, 0, 0, 0, 586_000, time.UTC)
	G := R.Add(7 * 24 * time.Hour)
	retentionFixture(t, ctx, tx, R, G, 3)
	legacyLoop(t, ctx, tx, R, G, 3)
	s := &Store{q: sqlc.New(tx)}
	deleteExpiredAll(t, ctx, s, R, 7)
	wm := graceWalk(t, ctx, s, floorMs(R), G, 3, 7)
	assertSameKeys(t, ctx, tx, "known_routes", "legacy_kr")
	for _, c := range []struct {
		at   time.Time
		want int
	}{{G.Add(-time.Microsecond), 0}, {floorMs(G), 0}, {G, 1}, {ceilMs(G), 1}} {
		if n := countAt(t, ctx, tx, c.at, 2); n != c.want {
			t.Errorf("obs-2 rows at %v = %d, want %d", c.at, n, c.want)
		}
	}
	if !wm.Equal(floorMs(G)) {
		t.Fatalf("watermark %v, want floor_ms(G) %v", wm, floorMs(G))
	}
	// Next run, one hour later: the walk starts at the stored watermark and takes the row at G.
	R2, G2 := R.Add(time.Hour), G.Add(time.Hour)
	legacyLoop(t, ctx, tx, R2, G2, 3)
	deleteExpiredAll(t, ctx, s, R2, 7)
	graceWalk(t, ctx, s, wm, G2, 3, 7)
	assertSameKeys(t, ctx, tx, "known_routes", "legacy_kr")
	if n := countAt(t, ctx, tx, G, 2); n != 0 {
		t.Errorf("obs-2 row at G survived the next run")
	}
}

func TestDeleteExpiredRoutesPostgres(t *testing.T) {
	whole := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for name, R := range map[string]time.Time{"whole ms": whole, "µs": whole.Add(586 * time.Microsecond)} {
		t.Run(name, func(t *testing.T) {
			ctx, tx := retentionTx(t)
			applyBaseline(t, ctx, tx)
			retentionFixture(t, ctx, tx, R, R.Add(7*24*time.Hour), 3)
			// Grace equal to retention: legacy deletes exactly the retention part.
			legacyLoop(t, ctx, tx, R, R, 3)
			before := countRows(t, ctx, tx, `SELECT count(*) FROM known_routes`)
			selected := deleteExpiredAll(t, ctx, &Store{q: sqlc.New(tx)}, R, 7)
			assertSameKeys(t, ctx, tx, "known_routes", "legacy_kr")
			if after := countRows(t, ctx, tx, `SELECT count(*) FROM known_routes`); int64(before-after) != selected || selected == 0 {
				t.Errorf("selected %d, deleted %d", selected, before-after)
			}
		})
	}

	t.Run("empty", func(t *testing.T) {
		ctx, tx := retentionTx(t)
		applyBaseline(t, ctx, tx)
		selected, last, err := (&Store{q: sqlc.New(tx)}).DeleteExpiredRoutes(ctx, RouteKey{}, whole, 7)
		if err != nil {
			t.Fatal(err)
		}
		if selected != 0 || last != (RouteKey{}) || last.ID != 0 {
			t.Errorf("empty table: selected %d, last %+v", selected, last)
		}
	})

	t.Run("grace at or before retention", func(t *testing.T) {
		ctx, tx := retentionTx(t)
		applyBaseline(t, ctx, tx)
		R := whole.Add(586 * time.Microsecond)
		G := R.Add(-24 * time.Hour)
		retentionFixture(t, ctx, tx, R, G, 3)
		legacyLoop(t, ctx, tx, R, G, 3)
		deleteExpiredAll(t, ctx, &Store{q: sqlc.New(tx)}, R, 7)
		assertSameKeys(t, ctx, tx, "known_routes", "legacy_kr")
	})
}

func TestDeleteGraceRoutesSlicePostgres(t *testing.T) {
	R := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t.Run("one window equals legacy grace", func(t *testing.T) {
		ctx, tx := retentionTx(t)
		applyBaseline(t, ctx, tx)
		G := R.Add(5*time.Hour + 586*time.Microsecond)
		retentionFixture(t, ctx, tx, R, G, 3)
		legacyLoop(t, ctx, tx, R, G, 3)
		s := &Store{q: sqlc.New(tx)}
		deleteExpiredAll(t, ctx, s, R, 7)
		after, calls := RouteKey{TS: R}, 0
		for ; ; calls++ {
			res, err := s.DeleteGraceRoutesSlice(ctx, after, G, 3, 7)
			if err != nil {
				t.Fatal(err)
			}
			if res.Unresolved != nil || res.Deleted != res.Candidates {
				t.Fatalf("unresolved %v, deleted %d of %d", res.Unresolved, res.Deleted, res.Candidates)
			}
			if !res.WindowEnd.Equal(ceilMs(G)) {
				t.Fatalf("WindowEnd %v, want ceil_ms(G)", res.WindowEnd)
			}
			if res.Candidates < 7 {
				break
			}
			after = res.Last
		}
		if calls == 0 {
			t.Fatal("expected more than one batch")
		}
		assertSameKeys(t, ctx, tx, "known_routes", "legacy_kr")
		for obs, want := range map[int64]int{2: 0, 3: 1, 4: 1} {
			if n := countAt(t, ctx, tx, G.Add(-time.Microsecond), obs); n != want {
				t.Errorf("obs-%d rows at G-1µs = %d, want %d", obs, n, want)
			}
		}
	})

	t.Run("window end", func(t *testing.T) {
		ctx, tx := retentionTx(t)
		applyBaseline(t, ctx, tx)
		G := R.Add(7 * 24 * time.Hour)
		retentionFixture(t, ctx, tx, R, G, 3)
		end := G.Add(-2 * time.Hour)
		insertRoutes(t, ctx, tx, []time.Time{G.Add(-5 * time.Hour), end.Add(-time.Microsecond), end}, []int64{1, 1, 1})
		mustExec(t, ctx, tx, `CREATE TABLE snap AS SELECT id, last_seen FROM known_routes`)
		s := &Store{q: sqlc.New(tx)}
		res, err := s.DeleteGraceRoutesSlice(ctx, RouteKey{TS: G.Add(-8 * time.Hour)}, G, 3, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !res.WindowEnd.Equal(end) {
			t.Fatalf("WindowEnd %v, want %v", res.WindowEnd, end)
		}
		if res.Deleted < 2 || res.Deleted != res.Candidates {
			t.Errorf("deleted %d of %d candidates", res.Deleted, res.Candidates)
		}
		var late int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM snap s WHERE NOT EXISTS (SELECT 1 FROM known_routes k WHERE k.id = s.id)
  AND date_trunc('milliseconds', s.last_seen, 'UTC') >= $1`, pgtype.Timestamptz{Time: end, Valid: true}).Scan(&late); err != nil {
			t.Fatal(err)
		}
		if late != 0 {
			t.Errorf("%d rows at or past WindowEnd deleted", late)
		}
		if countAt(t, ctx, tx, end, 1) != 1 || countAt(t, ctx, tx, end.Add(-time.Microsecond), 1) != 0 {
			t.Error("rows around WindowEnd: want the one at the bound kept and the one just before it deleted")
		}

		Gµ := G.Add(586 * time.Microsecond)
		res, err = s.DeleteGraceRoutesSlice(ctx, RouteKey{TS: floorMs(Gµ).Add(-time.Millisecond)}, Gµ, 3, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !res.WindowEnd.Equal(ceilMs(Gµ)) {
			t.Fatalf("WindowEnd %v, want ceil_ms(G) %v", res.WindowEnd, ceilMs(Gµ))
		}
	})

	t.Run("empty window", func(t *testing.T) {
		ctx, tx := retentionTx(t)
		applyBaseline(t, ctx, tx)
		G := R.Add(7*24*time.Hour + 586*time.Microsecond)
		retentionFixture(t, ctx, tx, R, G, 3)
		res, err := (&Store{q: sqlc.New(tx)}).DeleteGraceRoutesSlice(ctx, RouteKey{TS: G.Add(30 * 24 * time.Hour)}, G, 3, 7)
		if err != nil {
			t.Fatal(err)
		}
		if res.Candidates != 0 || res.Deleted != 0 || res.Last != (RouteKey{}) || res.Unresolved != nil || !res.WindowEnd.Equal(ceilMs(G)) {
			t.Errorf("empty window: %+v", res)
		}
	})
}

// A victim locked by another session is reported unresolved and deleted by the next walk from the held watermark.
func TestGraceWatermarkHeldByLockedRowPostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	G := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `DELETE FROM known_routes`); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `
INSERT INTO known_routes (id, path_key, node_ids, hash_prefix, iata, hop_count, last_seen, observation_count)
SELECT i, int8send(i), ARRAY[md5(i::text)::uuid], ARRAY['\x01'::bytea], 'SEA', 1,
       $1::timestamptz - (6 - least(i, 5)) * interval '30 minutes' - interval '30 minutes',
       CASE WHEN i = 6 THEN 5 ELSE 1 END
FROM generate_series(1, 6) i`, pgtype.Timestamptz{Time: G, Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	var lockedTS time.Time
	if err := pool.QueryRow(ctx, `SELECT last_seen FROM known_routes WHERE path_key = int8send(3)`).Scan(&lockedTS); err != nil {
		t.Fatal(err)
	}
	busy, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Rollback(context.Background())
	if _, err := busy.Exec(ctx, `SELECT 1 FROM known_routes WHERE path_key = int8send(3) FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	res, err := store.DeleteGraceRoutesSlice(ctx, RouteKey{TS: G.Add(-4 * time.Hour)}, G, 3, 100)
	if err != nil {
		t.Fatal(err)
	}
	if res.Candidates != 5 || res.Deleted != 4 || res.Unresolved == nil || !res.Unresolved.Equal(lockedTS) {
		t.Fatalf("with route 3 locked: %+v, want 5 candidates, 4 deleted, unresolved %v", res, lockedTS)
	}
	if err := busy.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	res, err = store.DeleteGraceRoutesSlice(ctx, RouteKey{TS: *res.Unresolved}, G, 3, 100)
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 1 || res.Unresolved != nil {
		t.Fatalf("after release: %+v, want 1 deleted, nothing unresolved", res)
	}
	var left []int64
	rows, err := pool.Query(ctx, `SELECT id FROM known_routes`)
	if err != nil {
		t.Fatal(err)
	}
	if left, err = pgx.CollectRows(rows, pgx.RowTo[int64]); err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 || left[0] != 6 {
		t.Errorf("routes left %v, want only the survivor 6", left)
	}
}
