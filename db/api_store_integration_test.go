// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
	"github.com/MeshCore-Beacon/beacon-server/internal/guards"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The API store as main builds it runs the known-routes hop pair unnamed on the real router, in
// every layout: neither statement ever becomes a named prepared statement on its API pool.
func TestAPIStoreHopStatementsUnnamedPostgres(t *testing.T) {
	dsn := schemaDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for _, layout := range []planpolicy.Layout{planpolicy.CustomAll, planpolicy.Split, planpolicy.Single} {
		t.Run(string(layout), func(t *testing.T) {
			// One connection per pool, so pg_prepared_statements on the pool sees the session that ran.
			pools, err := OpenPools(ctx, dsn, layout, func(_ string, cfg *pgxpool.Config) { cfg.MaxConns = 1 })
			if err != nil {
				t.Fatal(err)
			}
			defer pools.Close()
			if err := RunMigrations(ctx, pools.Maint); err != nil {
				t.Fatal(err)
			}
			guard := guards.New(guards.Options{Enforce: guards.Families, ForceObserve: layout == planpolicy.Single, Budget: "shadow"})
			store := NewAPIStore(pools, layout, guard, time.Hour, time.Hour)

			for range 7 {
				if _, err := store.ListKnownRoutes(ctx, nil, 2, time.Time{}, 0, 50); err != nil {
					t.Fatal(err)
				}
				if _, err := store.ListKnownRoutes(ctx, []string{"YYZ"}, 2, time.Time{}, 0, 50); err != nil {
					t.Fatal(err)
				}
			}

			apiPool := pools.APICustom
			if layout == planpolicy.Single {
				apiPool = pools.APILegacy
			}
			named := func(stmt string) int {
				t.Helper()
				var n int
				if err := apiPool.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_statements WHERE statement LIKE $1",
					"-- name: "+stmt+" %").Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}
			for _, stmt := range []string{"ListKnownRoutesGlobalHop", "ListKnownRoutesByIATAHop"} {
				if n := named(stmt); n != 0 {
					t.Errorf("%s: %d named prepared statements on the API pool, want 0", stmt, n)
				}
			}
			for _, p := range []*pgxpool.Pool{pools.APILegacy, pools.APICustom, pools.APICached} {
				if p == nil || p == apiPool {
					continue
				}
				var n int
				if err := p.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_statements WHERE statement LIKE '-- name: ListKnownRoutes%'").Scan(&n); err != nil {
					t.Fatal(err)
				}
				if n != 0 {
					t.Errorf("%d known-routes statements prepared on another API pool", n)
				}
			}

			// The detector sees a named statement: the same header run directly on the pool is prepared.
			if err := apiPool.QueryRow(ctx, "-- name: ListKnownRoutesGlobalHop :many\nSELECT $1::int", 2).Scan(new(int)); err != nil {
				t.Fatal(err)
			}
			if n := named("ListKnownRoutesGlobalHop"); n != 1 {
				t.Errorf("control: %d named prepared statements after a direct run, want 1", n)
			}
		})
	}
}
