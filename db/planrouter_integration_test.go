// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
	"github.com/MeshCore-Beacon/beacon-server/internal/guards"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaDSN returns the test DSN with search_path set to a fresh schema, dropped on cleanup.
func schemaDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL regression")
	}
	ctx := context.Background()
	setup, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "planrouter_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := setup.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := setup.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
		setup.Close(context.Background())
	})
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "search_path=" + schema
}

// setupProbe creates probe(a): 100,000 rows, a = 1 except every 100th row, which holds its row number.
func setupProbe(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, `CREATE TABLE probe(a int);
INSERT INTO probe SELECT CASE WHEN i % 100 = 0 THEN i ELSE 1 END FROM generate_series(1, 100000) i;
CREATE INDEX ON probe(a);
ANALYZE probe;`); err != nil {
		t.Fatal(err)
	}
}

// planPart returns the plan of an auto_explain JSON notice, without its "Query Text" and
// "Query Parameters", which always show $1.
func planPart(msg string) string {
	_, body, ok := strings.Cut(msg, "\n")
	if !ok {
		return msg
	}
	var v struct{ Plan json.RawMessage }
	if err := json.Unmarshal([]byte(body), &v); err != nil || v.Plan == nil {
		return msg
	}
	return string(v.Plan)
}

// newPlanRouterFromPools routes over p with the given classes; names absent from classes take
// the real policy.
func newPlanRouterFromPools(p *Pools, layout planpolicy.Layout, gate guards.Gate, classes map[string]planpolicy.Class) *PlanRouter {
	classOf := func(name string) planpolicy.Class {
		if c, ok := classes[name]; ok {
			return c
		}
		return planpolicy.ClassOf(name)
	}
	return newPlanRouter(wrapPool(p.APILegacy, PoolAPILegacy), wrapPool(p.APICustom, PoolAPICustom),
		wrapPool(p.APICached, PoolAPICached), layout, gate, classOf)
}

// A class C statement run 7 times on one pooled connection never becomes a named prepared
// statement and never plans generic, under every layout. A class G statement under split does
// plan generic, which shows the detector works.
func TestPlanRouterClassCUnnamedPostgres(t *testing.T) {
	dsn := schemaDSN(t)
	setupProbe(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const probeC = "-- name: ProbeC :one\nSELECT count(*) FROM probe WHERE a = $1"
	const probeG = "-- name: ProbeG :one\nSELECT count(*) FROM probe WHERE a = $1"

	for _, layout := range []planpolicy.Layout{planpolicy.CustomAll, planpolicy.Split, planpolicy.Single} {
		t.Run(string(layout), func(t *testing.T) {
			var mu sync.Mutex
			var plans []string
			takePlans := func() []string {
				mu.Lock()
				defer mu.Unlock()
				out := plans
				plans = nil
				return out
			}
			p, err := OpenPools(ctx, dsn, layout, func(_ string, cfg *pgxpool.Config) {
				cfg.MaxConns = 1
				cfg.ConnConfig.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
					mu.Lock()
					plans = append(plans, planPart(n.Message))
					mu.Unlock()
				}
				cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
					_, err := c.Exec(ctx, "LOAD 'auto_explain'; SET auto_explain.log_min_duration = 0; "+
						"SET auto_explain.log_level = notice; SET auto_explain.log_format = json")
					return err
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			r := newPlanRouterFromPools(p, layout, guards.NopGate, map[string]planpolicy.Class{"ProbeG": planpolicy.ClassG})

			// run executes sql 7 times and returns the captured plans.
			run := func(sql string) []string {
				t.Helper()
				takePlans()
				for range 7 {
					var n int64
					if err := r.QueryRow(ctx, sql, 500).Scan(&n); err != nil {
						t.Fatal(err)
					}
					if n != 1 {
						t.Fatalf("count = %d, want 1", n)
					}
				}
				got := takePlans()
				if len(got) != 7 {
					t.Fatalf("captured %d plans, want 7: %q", len(got), got)
				}
				return got
			}
			prepared := func(pool *pgxpool.Pool, name string) int {
				t.Helper()
				var n int
				if err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_statements WHERE statement LIKE $1",
					"-- name: "+name+"%").Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}

			cPool := p.APICustom
			if layout == planpolicy.Single {
				cPool = p.APILegacy
			}
			for i, plan := range run(probeC) {
				if strings.Contains(plan, "$1") {
					t.Errorf("ProbeC execution %d planned generic: %s", i+1, plan)
				}
			}
			if n := prepared(cPool, "ProbeC"); n != 0 {
				t.Errorf("ProbeC: %d named prepared statements, want 0", n)
			}

			switch layout {
			case planpolicy.Split:
				for i, plan := range run(probeG) {
					if !strings.Contains(plan, "$1") {
						t.Errorf("ProbeG execution %d on api_cached planned custom: %s", i+1, plan)
					}
				}
				if n := prepared(p.APICached, "ProbeG"); n != 1 {
					t.Errorf("ProbeG on api_cached: %d prepared statements, want 1", n)
				}
			case planpolicy.CustomAll:
				for i, plan := range run(probeG) {
					if strings.Contains(plan, "$1") {
						t.Errorf("ProbeG execution %d on api_custom planned generic: %s", i+1, plan)
					}
				}
				if n := prepared(p.APICustom, "ProbeG"); n != 1 {
					t.Errorf("ProbeG on api_custom: %d prepared statements, want 1", n)
				}
			}
		})
	}
}
