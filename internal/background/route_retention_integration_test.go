// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// legacyDeleteOldRoutes is the pre-change statement, DeleteOldRoutes as sqlc generated it, run on
// legacy_kr. It is a copy of the db package's test oracle, which test code here cannot import.
const legacyDeleteOldRoutes = `WITH expired AS (
    SELECT r.iata, r.path_key
    FROM legacy_kr r
    WHERE r.last_seen < GREATEST($1::timestamptz, $2::timestamptz)
      AND (r.last_seen < $1 OR
           (r.observation_count < $3 AND r.last_seen < $2))
    LIMIT $4
    FOR UPDATE OF r SKIP LOCKED
)
DELETE FROM legacy_kr kr USING expired e
WHERE kr.iata = e.iata AND kr.path_key = e.path_key`

// migratedPool returns a pool on a fresh schema built by db.RunMigrations, dropped on cleanup.
func migratedPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"route_retention_" + strings.ReplaceAll(uuid.NewString(), "-", "")}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close(context.Background())
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return ctx, pool
}

// graceStartRecorder is the real store; it records the first grace slice's start (reset per run).
type graceStartRecorder struct {
	*db.Store
	start *time.Time
}

func (r *graceStartRecorder) DeleteGraceRoutesSlice(ctx context.Context, after db.RouteKey, G time.Time, m int64, batch int32) (db.GraceSliceResult, error) {
	if r.start == nil {
		r.start = &after.TS
	}
	return r.Store.DeleteGraceRoutesSlice(ctx, after, G, m, batch)
}

// TestRouteRetentionTaskPostgres drives the production task on a real store, a full pass and then
// an incremental run, and checks the deleted set against the pre-change statement after each.
func TestRouteRetentionTaskPostgres(t *testing.T) {
	ctx, pool := migratedPool(t)
	const (
		retention       = 14 * 24 * time.Hour
		grace           = 7 * 24 * time.Hour
		m         int64 = 3
		batch     int32 = 7 // small, so both loops page within windows
	)
	now1 := time.Date(2026, 1, 15, 12, 34, 56, 586_400, time.UTC)
	now2 := now1.Add(time.Hour + 123_456_700) // +1h0m0.1234567s: incremental, new cutoffs
	cut := func(now time.Time) (R, G time.Time) { return now.Add(-retention), now.Add(-grace) }
	floorMs := func(t time.Time) time.Time { return t.Truncate(time.Millisecond) }
	ceilMs := func(t time.Time) time.Time {
		t = t.Truncate(time.Microsecond)
		if f := floorMs(t); f.Before(t) {
			return f.Add(time.Millisecond)
		}
		return t
	}

	var ts []time.Time
	var obs []int64
	add := func(at time.Time, o int64) { ts, obs = append(ts, at), append(obs, o) }
	edges := func(cs ...time.Time) {
		for _, c := range cs {
			for _, at := range []time.Time{c.Add(-time.Microsecond), floorMs(c), c, c.Add(time.Microsecond), ceilMs(c)} {
				for _, o := range []int64{m - 1, m, m + 1} {
					add(at, o)
				}
			}
		}
	}
	insert := func() {
		t.Helper()
		_, err := pool.Exec(ctx, `
INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count, last_seen, observation_count)
SELECT int8send(b.base + u.n), ARRAY[md5((b.base + u.n)::text)::uuid], ARRAY['\x01'::bytea], 'YYZ', 1, u.ts, u.obs
FROM (SELECT coalesce(max(id), 0) AS base FROM known_routes) b,
     unnest($1::timestamptz[], $2::bigint[]) WITH ORDINALITY AS u(ts, obs, n)`, ts, obs)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO legacy_kr SELECT * FROM known_routes k WHERE NOT EXISTS (SELECT 1 FROM legacy_kr l WHERE l.id = k.id)`); err != nil {
			t.Fatal(err)
		}
		ts, obs = nil, nil
	}
	legacy := func(R, G time.Time) {
		t.Helper()
		for {
			tag, err := pool.Exec(ctx, legacyDeleteOldRoutes, pgtype.Timestamptz{Time: R, Valid: true}, pgtype.Timestamptz{Time: G, Valid: true}, m, int64(10_000))
			if err != nil {
				t.Fatal(err)
			}
			if tag.RowsAffected() < 10_000 {
				return
			}
		}
	}
	same := func(run string) {
		t.Helper()
		var extra, missing, left int
		if err := pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM (SELECT id FROM known_routes EXCEPT SELECT id FROM legacy_kr) x),
  (SELECT count(*) FROM (SELECT id FROM legacy_kr EXCEPT SELECT id FROM known_routes) x),
  (SELECT count(*) FROM known_routes)`).Scan(&extra, &missing, &left); err != nil {
			t.Fatal(err)
		}
		if extra != 0 || missing != 0 {
			t.Fatalf("%s: %d rows kept that the pre-change statement deletes, %d deleted that it keeps (%d left)", run, extra, missing, left)
		}
	}
	saved := func() (routesGraceState, string) {
		t.Helper()
		var raw []byte
		if err := pool.QueryRow(ctx, `SELECT value FROM background_state WHERE key = $1`, RoutesGraceKey).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var st routesGraceState
		if err := json.Unmarshal(raw, &st); err != nil {
			t.Fatal(err)
		}
		return st, string(raw)
	}

	if _, err := pool.Exec(ctx, `CREATE TABLE legacy_kr (LIKE known_routes INCLUDING ALL);
INSERT INTO iata_codes (iata) VALUES ('YYZ');
INSERT INTO nodes (id, public_key, node_type) VALUES
  ('00000000-0000-0000-0000-000000000001', '\x01', 2), ('00000000-0000-0000-0000-000000000002', '\x02', 2);
INSERT INTO node_neighbors (node_id, neighbor_id, iata) VALUES
  ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'YYZ')`); err != nil {
		t.Fatal(err)
	}

	// Run 1: expiry, grace and well-observed rows around R1 and G1, including G1-1µs, G1 and trunc_ms(G1).
	R1, G1 := cut(now1)
	for i := 1; i <= 30; i++ {
		add(R1.Add(-time.Duration(i)*37*time.Minute), 1+int64(i)%5)
	}
	edges(R1, G1)
	for i := range 25 { // a µs tie group inside one ms below G1
		add(floorMs(G1).Add(-3*time.Millisecond+time.Duration(i)*time.Microsecond), 1)
	}
	for i := 1; i <= 60; i++ { // spread between R1 and G1, crossing many 6h windows
		add(R1.Add(grace*time.Duration(i)/61), 1+int64(i)%(m+1))
	}
	for i := 1; i <= 10; i++ {
		add(G1.Add(time.Duration(i)*7*time.Minute), 1)
	}
	insert()

	store := &graceStartRecorder{Store: db.New(pool, time.Hour, time.Hour)}
	if err := routeRetentionTask(store, retention, grace, m, time.Hour, func() time.Time { return now1 }, batch).Run(ctx); err != nil {
		t.Fatal(err)
	}
	legacy(R1, G1)
	same("full pass")
	if store.start == nil || !store.start.Equal(floorMs(R1)) {
		t.Fatalf("full pass started at %v, want floor_ms(R) %v", store.start, floorMs(R1))
	}
	st, raw := saved()
	want := routesGraceState{Watermark: floorMs(G1), MinObservations: m, Grace: "168h0m0s", LastFullPass: now1.Truncate(time.Hour)}
	if !st.Watermark.Equal(want.Watermark) || st.MinObservations != m || st.Grace != want.Grace || !st.LastFullPass.Equal(want.LastFullPass) {
		t.Fatalf("saved %s, want %+v", raw, want)
	}
	var neighbors int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM node_neighbors`).Scan(&neighbors); err != nil || neighbors != 0 {
		t.Fatalf("departed neighbor left: %d, %v", neighbors, err)
	}

	// Run 2: rows seen since run 1 (last_seen only moves forward) around G2, then an incremental run.
	R2, G2 := cut(now2)
	edges(G2)
	for i := 1; i <= 40; i++ {
		add(G1.Add((G2.Sub(G1))*time.Duration(i)/41), 1+int64(i)%(m+1))
	}
	insert()
	store.start = nil
	if err := routeRetentionTask(store, retention, grace, m, time.Hour, func() time.Time { return now2 }, batch).Run(ctx); err != nil {
		t.Fatal(err)
	}
	legacy(R2, G2)
	same("incremental")
	if store.start == nil || !store.start.Equal(floorMs(G1)) {
		t.Fatalf("incremental run started at %v, want the saved watermark %v", store.start, floorMs(G1))
	}
	st, raw = saved()
	if !st.Watermark.Equal(floorMs(G2)) || !st.LastFullPass.Equal(want.LastFullPass) || st.Grace != want.Grace || st.MinObservations != m {
		t.Fatalf("saved %s, want watermark %v and LastFullPass carried %v", raw, floorMs(G2), want.LastFullPass)
	}
}
