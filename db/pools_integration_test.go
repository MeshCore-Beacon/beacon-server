// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
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
		PoolAPICustom: {"beacon-api_custom", "force_custom_plan", "off", "0", "35s", "30s"},
		PoolAPICached: {"beacon-api_cached", "force_generic_plan", "off", "0", "35s", "30s"},
		PoolIngest:    {"beacon-ingest", "auto", "off", dflt, "30s", "0"},
		PoolMaint:     {"beacon-maint", "force_custom_plan", "off", "0", "0", "0"},
	}
	shows := []string{"application_name", "plan_cache_mode", "jit", "max_parallel_workers_per_gather",
		"statement_timeout", "idle_in_transaction_session_timeout"}

	for _, tc := range []struct {
		layout planpolicy.Layout
		want   map[string]int32
		total  int32
	}{
		{planpolicy.CustomAll, map[string]int32{"api_legacy": 8, "api_custom": 8, "ingest": 6, "maint": 4}, 26},
		{planpolicy.Split, map[string]int32{"api_legacy": 8, "api_custom": 7, "api_cached": 5, "ingest": 6, "maint": 4}, 30},
		{planpolicy.Single, map[string]int32{"api_legacy": 8, "ingest": 6, "maint": 4}, 18},
	} {
		t.Run(string(tc.layout), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			p, err := OpenPools(ctx, dsn, tc.layout)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			for name, pool := range map[string]*pgxpool.Pool{
				PoolAPILegacy: p.APILegacy, PoolAPICustom: p.APICustom, PoolAPICached: p.APICached,
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
				conn, err := pool.Acquire(ctx)
				if err != nil {
					t.Fatalf("%s: acquire: %v", name, err)
				}
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
				conn.Release()
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
