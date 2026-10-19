// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	pgdata "github.com/MeshCore-Beacon/beacon-server/db/planguard"
	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// throwawayDB creates database name on the test cluster, drops it on cleanup, and returns a pool on it.
func throwawayDB(t *testing.T, ctx context.Context, name string) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL tests")
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	drop := func() error {
		_, err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		return err
	}
	if err := drop(); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if err := drop(); err != nil {
			t.Error(err)
		}
		admin.Close(context.Background())
	})
	return pool
}

// planRows returns the planner's row estimate for the top node of query.
func planRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string) float64 {
	t.Helper()
	var raw string
	if err := pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+query).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var doc []struct {
		Plan struct {
			Rows float64 `json:"Plan Rows"`
		} `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil || len(doc) != 1 {
		t.Fatalf("EXPLAIN %s: %v (%s)", query, err, raw)
	}
	return doc[0].Plan.Rows
}

// fixtureDB returns a pool on a built fixture. With PLANGUARD_FIXTURE_DSN set (CI, after the fixture-tier step
// built it) it reuses that database, after checking its marker records a finished build of these very files;
// otherwise it builds one in a fresh database.
func fixtureDB(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PLANGUARD_FIXTURE_DSN")
	if dsn == "" {
		pool := throwawayDB(t, ctx, "planguard_fixture_test")
		t0 := time.Now()
		if err := planguard.BuildFixture(ctx, pool, pgdata.Files); err != nil {
			t.Fatal(err)
		}
		t.Logf("fixture built in %.1f s", time.Since(t0).Seconds())
		return pool
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	want, err := planguard.FixtureHash(pgdata.Files)
	if err != nil {
		t.Fatal(err)
	}
	var got string
	if err := pool.QueryRow(ctx, "SELECT coalesce(max(hash) FILTER (WHERE built_at IS NOT NULL), '') FROM planguard_src.fixture_marker").Scan(&got); err != nil {
		t.Fatalf("PLANGUARD_FIXTURE_DSN: no fixture marker: %v", err)
	}
	if got != want {
		t.Fatalf("PLANGUARD_FIXTURE_DSN holds a fixture of hash %q, want a finished build of these files (%s)", got, want)
	}
	t.Logf("reusing the fixture built from these files (%s)", want)
	return pool
}

func TestBuildFixtureThroughMigrationsPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	testDSN := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if testDSN == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL tests")
	}
	pool := fixtureDB(t, ctx)

	// Every embedded migration is applied.
	files, err := filepath.Glob("../../db/migrations/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob migrations: %v (%d files)", err, len(files))
	}
	var applied []string
	rows, err := pool.Query(ctx, "SELECT filename FROM schema_migrations ORDER BY filename")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			t.Fatal(err)
		}
		applied = append(applied, f)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if !slices.Contains(applied, filepath.Base(f)) {
			t.Errorf("migration %s not applied", filepath.Base(f))
		}
	}

	var regions int
	if err := pool.QueryRow(ctx, "SELECT count(DISTINCT iata) FROM known_routes").Scan(&regions); err != nil {
		t.Fatal(err)
	}
	if regions < 300 {
		t.Errorf("known_routes covers %d regions, want >= 300", regions)
	}

	// The fixture adds no index: known_routes carries exactly the migrations' set, read from a schema migrated on
	// its own in the test database.
	schema := "planguard_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	cfg, err := pgxpool.ParseConfig(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 1
	ref, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ref.Close()
	if _, err := ref.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := ref.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	if err := db.RunMigrations(ctx, ref); err != nil {
		t.Fatal(err)
	}
	indexes := func(p *pgxpool.Pool, ns string) []string {
		var out []string
		rows, err := p.Query(ctx, "SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = 'known_routes' ORDER BY 1", ns)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				t.Fatal(err)
			}
			out = append(out, n)
		}
		return out
	}
	if got, want := indexes(pool, "public"), indexes(ref, schema); !slices.Equal(got, want) || len(want) == 0 {
		t.Errorf("known_routes indexes %v, want the migrations' %v", got, want)
	}

	// The stale region: thousands of rows the statistics have never seen.
	var stale int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM known_routes WHERE iata = 'NEW'").Scan(&stale); err != nil {
		t.Fatal(err)
	}
	if stale < 2000 {
		t.Errorf("region NEW holds %d routes, want >= 2,000", stale)
	}
	for _, q := range []string{
		"SELECT * FROM known_routes WHERE iata = 'NEW'",
		"SELECT * FROM known_routes WHERE iata >= 'NEW' AND iata <= 'NEW'",
	} {
		if est := planRows(t, ctx, pool, q); est > 10 {
			t.Errorf("%s: estimated %.0f rows, want <= 10 (statistics taken before the region was loaded)", q, est)
		}
	}

	// A second build finds the marker and returns.
	t1 := time.Now()
	if err := planguard.BuildFixture(ctx, pool, pgdata.Files); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(t1); d >= 2*time.Second {
		t.Errorf("second BuildFixture took %s, want < 2 s (marker hit)", d)
	}
}

func TestBuildFixtureRefusesRealDatabasePostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := throwawayDB(t, ctx, "planguard_refuse_test")
	if err := db.RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload,
  raw_header, first_heard_at, last_heard_at) VALUES ('\x0102030405060708', 4, 0, 1, '\x00', '\x00', now(), now())`); err != nil {
		t.Fatal(err)
	}
	var migrations int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&migrations); err != nil {
		t.Fatal(err)
	}

	err := planguard.BuildFixture(ctx, pool, pgdata.Files)
	if err == nil || !strings.Contains(err.Error(), "not a planguard fixture database") {
		t.Fatalf("BuildFixture on a database with packets = %v, want %q", err, "not a planguard fixture database")
	}

	// Nothing was written.
	var packets, routes, after int
	var src bool
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM packets), (SELECT count(*) FROM known_routes),
  (SELECT count(*) FROM schema_migrations), to_regnamespace('planguard_src') IS NOT NULL`).Scan(&packets, &routes, &after, &src); err != nil {
		t.Fatal(err)
	}
	if packets != 1 || routes != 0 || after != migrations || src {
		t.Errorf("after the refusal: packets %d, known_routes %d, migrations %d (was %d), planguard_src exists %v; want 1, 0, unchanged, false",
			packets, routes, after, migrations, src)
	}
}
