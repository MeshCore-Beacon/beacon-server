// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Each opened pool has exactly the table's settings and size; pools the layout does not use are nil.
func TestOpenPoolsSettingsPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	// A parameter in the DSN must survive the per-pool settings merge.
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	dsn += sep + "search_path=pools_test_path"

	const dflt = "-" // not asserted: the server default
	// application_name, plan_cache_mode, jit, max_parallel_workers_per_gather, statement_timeout,
	// idle_in_transaction_session_timeout
	settings := map[string][6]string{
		PoolAPILegacy: {"beacon-api_legacy", "auto", dflt, dflt, "0", "0"},
		PoolAPISlow:   {"beacon-api_slow", "auto", dflt, "0", "2min", "0"},
		PoolAPICustom: {"beacon-api_custom", "force_custom_plan", "off", "0", "35s", "30s"},
		PoolAPICached: {"beacon-api_cached", "force_generic_plan", "off", "0", "35s", "30s"},
		PoolIngest:    {"beacon-ingest", "auto", "off", dflt, "0", "0"},
		PoolMaint:     {"beacon-maint", "force_custom_plan", "off", "0", "0", "0"},
	}
	shows := []string{"application_name", "plan_cache_mode", "jit", "max_parallel_workers_per_gather",
		"statement_timeout", "idle_in_transaction_session_timeout"}

	for _, tc := range []struct {
		layout planpolicy.Layout
		want   map[string]int32
		total  int32
	}{
		{planpolicy.CustomAll, map[string]int32{"api_slow": 2, "api_custom": 8, "maint": 4}, 0},
		{planpolicy.Split, map[string]int32{"api_slow": 2, "api_custom": 7, "api_cached": 5, "maint": 4}, 0},
		{planpolicy.Single, map[string]int32{"api_legacy": 8, "maint": 4}, 0},
	} {
		// The ingest pool follows the host: at least 6, and one per CPU.
		tc.want["ingest"] = int32(max(6, runtime.NumCPU()))
		for _, n := range tc.want {
			tc.total += n
		}
		t.Run(string(tc.layout), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			p, err := OpenPools(ctx, dsn, tc.layout)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			for name, pool := range map[string]*pgxpool.Pool{
				PoolAPILegacy: p.APILegacy, PoolAPISlow: p.APISlow, PoolAPICustom: p.APICustom, PoolAPICached: p.APICached,
				PoolIngest: p.Ingest, PoolMaint: p.Maint,
			} {
				size, opened := tc.want[name]
				if (pool != nil) != opened {
					t.Errorf("%s: opened = %v, want %v", name, pool != nil, opened)
					continue
				}
				if !opened {
					continue
				}
				if got := pool.Config().MaxConns; got != size {
					t.Errorf("%s: MaxConns = %d, want %d", name, got, size)
				}
				// A failed assertion must release the connection, or the deferred Close waits on it.
				func() {
					conn, err := pool.Acquire(ctx)
					if err != nil {
						t.Fatalf("%s: acquire: %v", name, err)
					}
					defer conn.Release()
					for i, guc := range shows {
						want := settings[name][i]
						if want == dflt {
							continue
						}
						var got string
						if err := conn.QueryRow(ctx, "SHOW "+guc).Scan(&got); err != nil {
							t.Fatalf("%s: SHOW %s: %v", name, guc, err)
						}
						if got != want {
							t.Errorf("%s: %s = %q, want %q", name, guc, got, want)
						}
					}
					var path string
					if err := conn.QueryRow(ctx, "SHOW search_path").Scan(&path); err != nil {
						t.Fatal(err)
					}
					if path != "pools_test_path" {
						t.Errorf("%s: search_path = %q, want the DSN's pools_test_path", name, path)
					}
					// The tracer labels the statement with its sqlc name, and Query ends at rows.Close.
					rows, err := conn.Query(ctx, "-- name: PoolsTracerProbe :many\nSELECT 1")
					if err != nil {
						t.Fatal(err)
					}
					rows.Close()
				}()
				body := scrapeMetrics(t)
				for _, want := range []string{
					`beacon_db_time_seconds_count{pool="` + name + `",statement="PoolsTracerProbe"}`,
					`beacon_db_time_seconds_count{pool="` + name + `",statement="unnamed"}`,
					`beacon_pool_acquire_seconds_count{pool="` + name + `"}`,
				} {
					if !strings.Contains(body, want) {
						t.Errorf("metrics missing %s", want)
					}
				}
			}
			if got := p.Total(); got != tc.total {
				t.Errorf("Total() = %d, want %d", got, tc.total)
			}
			// Without ingest (6 on up to 6 CPUs): custom_all 20 in all, split 24, single 18.
			if rest := tc.total - tc.want["ingest"]; rest != map[planpolicy.Layout]int32{planpolicy.CustomAll: 14, planpolicy.Split: 18, planpolicy.Single: 12}[tc.layout] {
				t.Errorf("%s: %d connections besides ingest", tc.layout, rest)
			}
		})
	}
}

func scrapeMetrics(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	b, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// A tune that sets default_transaction_read_only on every pool makes each pool refuse a write
// with SQLSTATE 25006 while reads still work.
func TestReadOnlyPoolsRejectWritesPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// Own throwaway database, so the table exists and nothing else sees the attempt.
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	name := "beacon_read_only_pools_" + strings.ToLower(rand.Text())
	ident := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+ident); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP DATABASE "+ident+" WITH (FORCE)")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	migrated, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = RunMigrations(ctx, migrated)
	migrated.Close()
	if err != nil {
		t.Fatal(err)
	}

	tuned := map[string]bool{}
	tune := func(pool string, c *pgxpool.Config) {
		tuned[pool] = true
		c.ConnConfig.Database = name
		c.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	}
	p, err := OpenPools(ctx, dsn, planpolicy.CustomAll, tune)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	pools := map[string]*pgxpool.Pool{PoolAPISlow: p.APISlow, PoolAPICustom: p.APICustom, PoolIngest: p.Ingest, PoolMaint: p.Maint}
	for name, pool := range pools {
		if !tuned[name] {
			t.Errorf("%s: tune not called", name)
		}
		_, err := pool.Exec(ctx, "INSERT INTO background_state(key, value) VALUES ($1, '{}')", "read-only-"+name)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "25006" {
			t.Errorf("%s: INSERT error = %v, want SQLSTATE 25006", name, err)
		}
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM background_state").Scan(&n); err != nil {
			t.Errorf("%s: SELECT: %v", name, err)
		} else if n != 0 {
			t.Errorf("%s: background_state has %d rows, want 0", name, n)
		}
	}
	if len(tuned) != len(pools) {
		t.Errorf("tune called for %v, want the %d custom_all pools", tuned, len(pools))
	}
}

// With both api_slow connections busy, a slow statement waits at most its acquire limit and fails with
// ErrSlowPoolBusy; it never takes an api_custom connection, and class C statements keep running there.
func TestSlowPoolIsolationPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	old := slowAcquireWait
	slowAcquireWait = 300 * time.Millisecond
	t.Cleanup(func() { slowAcquireWait = old })
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, err := OpenPools(ctx, dsn, planpolicy.CustomAll)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	r := NewPlanRouter(p, planpolicy.CustomAll, nil)
	const slowSQL = "-- name: ListPacketsByIATAs :many\nSELECT current_setting('application_name')"

	var app string
	if err := r.QueryRow(ctx, slowSQL).Scan(&app); err != nil || app != "beacon-api_slow" {
		t.Fatalf("slow statement ran on %q, %v; want beacon-api_slow", app, err)
	}
	var held []*pgxpool.Conn
	for range 2 {
		c, err := p.APISlow.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	customBefore := p.APICustom.Stat().AcquireCount()
	start := time.Now()
	err = r.QueryRow(ctx, slowSQL).Scan(&app)
	if !errors.Is(err, api.ErrSlowPoolBusy) {
		t.Fatalf("third slow statement err = %v, want ErrSlowPoolBusy", err)
	}
	if d := time.Since(start); d < 300*time.Millisecond || d > 5*time.Second {
		t.Errorf("third slow statement failed after %v, want about 300ms", d)
	}
	if n := p.APICustom.Stat().AcquireCount(); n != customBefore {
		t.Errorf("api_custom acquired %d connections for a slow statement", n-customBefore)
	}
	if err := r.QueryRow(ctx, "-- name: ProbeC :one\nSELECT current_setting('application_name')").Scan(&app); err != nil || app != "beacon-api_custom" {
		t.Errorf("class C statement while api_slow is full: %q, %v", app, err)
	}
	for _, c := range held {
		c.Release()
	}
	if err := r.QueryRow(ctx, slowSQL).Scan(&app); err != nil {
		t.Errorf("slow statement after release: %v", err)
	}
}

// SlowPoolSize resizes api_slow, and only api_slow; the single layout opens no api_slow to resize.
func TestSlowPoolSizePostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, layout := range []planpolicy.Layout{planpolicy.CustomAll, planpolicy.Split, planpolicy.Single} {
		p, err := OpenPools(ctx, dsn, layout, SlowPoolSize(8))
		if err != nil {
			t.Fatal(err)
		}
		if layout == planpolicy.Single {
			if p.APISlow != nil || p.APILegacy.Config().MaxConns != 8 {
				t.Errorf("single: api_slow %v, api_legacy %d", p.APISlow, p.APILegacy.Config().MaxConns)
			}
		} else if got := p.APISlow.Config().MaxConns; got != 8 || p.APICustom.Config().MaxConns > 8 {
			t.Errorf("%s: api_slow MaxConns = %d, want 8; api_custom %d", layout, got, p.APICustom.Config().MaxConns)
		}
		p.Close()
	}
}
