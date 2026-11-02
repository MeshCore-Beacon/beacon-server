// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"context"
	"io/fs"
	"os"
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

// prodEnv is a migrated, empty schema and a one-connection pool on it shaped like the maintenance pool.
type prodEnv struct {
	ctx    context.Context
	dsn    string
	schema string
	pool   *pgxpool.Pool
}

func newProdEnv(t *testing.T) *prodEnv {
	t.Helper()
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "planguard_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	cfg.ConnConfig.RuntimeParams["plan_cache_mode"] = "force_custom_plan"
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return &prodEnv{ctx: ctx, dsn: dsn, schema: schema, pool: pool}
}

// checker loads the embedded corpus, or the corpus.yaml in dir.
func (e *prodEnv) checker(t *testing.T, dir string) *planguard.ProdChecker {
	t.Helper()
	var corpus fs.FS = pgdata.Files
	if dir != "" {
		corpus = os.DirFS(dir)
	}
	p, err := planguard.NewProdChecker(e.pool, db.SQLCSources, corpus)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// pooled reads one value on the pool's own (single) connection.
func (e *prodEnv) pooled(t *testing.T, sql string) string {
	t.Helper()
	var v string
	if err := e.pool.QueryRow(e.ctx, sql).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestPlanguardProdTierPostgres(t *testing.T) {
	e := newProdEnv(t)
	cacheMode, path := e.pooled(t, "SHOW plan_cache_mode"), e.pooled(t, "SHOW search_path")
	pid := e.pooled(t, "SELECT pg_backend_pid()::text")

	// 1) The empty migrated schema. The embedded corpus plans every entry in custom and generic only, never
	//    analyzed; its plan shapes are measured on production-sized tables, so verdicts here are not judged, but no
	//    run may fail to plan.
	rep, err := e.checker(t, "").Run(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Tier != "prod" || len(rep.Results) == 0 {
		t.Fatalf("tier %s, %d runs", rep.Tier, len(rep.Results))
	}
	if !contains(rep.Indexes, "idx_known_routes_iata_keyset") {
		t.Errorf("indexes = %v: the run did not plan in the pool's search_path", rep.Indexes)
	}
	for _, r := range rep.Results {
		if r.Mode != planguard.ModeCustom && r.Mode != planguard.ModeGeneric {
			t.Errorf("%s|%s: the prod tier plans custom and generic only", r.ID, r.Mode)
		}
		if r.Verdict == planguard.VerdictError {
			t.Errorf("empty schema: %s|%s %v", r.ID, r.Mode, r.Failures)
		}
		if r.ExecMs != 0 || r.Buffers != 0 {
			t.Errorf("%s|%s: analyzed on the prod tier (exec %.2f ms, buffers %d)", r.ID, r.Mode, r.ExecMs, r.Buffers)
		}
	}
	if api := rep.API(); api.Failures == nil || len(api.Runs) != len(rep.Results) {
		t.Errorf("API: failures %v runs %d", api.Failures, len(api.Runs))
	}

	// The test corpus on the empty schema: a literal region plans as an empty region, a NULL resolver skips.
	p := e.checker(t, "testdata/prodcorpus")
	if rep, err = p.Run(e.ctx); err != nil {
		t.Fatal(err)
	}
	checkProdRuns(t, "empty schema", rep)

	// 2) ~2,000 routes over three regions: the routes entries plan ok, and the pooled session is as it was.
	if _, err := e.pool.Exec(e.ctx, `
INSERT INTO iata_codes (iata) VALUES ('SEA'), ('YVR'), ('PDX');
INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count, last_seen, observation_count)
SELECT int8send(g), ARRAY[md5(g::text)::uuid], ARRAY['\x01'::bytea], (ARRAY['SEA','YVR','PDX'])[1 + g % 3],
       1 + g % 5, timestamptz '2026-03-01 00:00:00+00' - g * interval '1 minute', 1 + g % 4
FROM generate_series(1, 2000) g;
ANALYZE known_routes`); err != nil {
		t.Fatal(err)
	}
	if rep, err = p.Run(e.ctx); err != nil {
		t.Fatal(err)
	}
	checkProdRuns(t, "2,000 routes", rep)
	if api := rep.API(); !api.Clean || len(api.Failures) != 0 || api.Counts["ok"] == 0 {
		t.Errorf("API: clean %v counts %v failures %v", api.Clean, api.Counts, api.Failures)
	}
	if got := e.pooled(t, "SELECT pg_backend_pid()::text"); got != pid {
		t.Errorf("pooled connection replaced after a clean run (pid %s → %s)", pid, got)
	}
	if n := e.pooled(t, "SELECT count(*)::text FROM pg_prepared_statements WHERE name LIKE 'planguard%'"); n != "0" {
		t.Errorf("%s planguard prepared statements left on the pooled connection", n)
	}
	if m := e.pooled(t, "SHOW plan_cache_mode"); m != cacheMode {
		t.Errorf("plan_cache_mode %s → %s", cacheMode, m)
	}
	if sp := e.pooled(t, "SHOW search_path"); sp != path {
		t.Errorf("search_path %s → %s", path, sp)
	}
	if ro := e.pooled(t, "SHOW transaction_read_only"); ro != "off" {
		t.Errorf("transaction_read_only = %s on the pooled connection", ro)
	}

	// 3) A transaction holding ACCESS EXCLUSIVE on known_routes: the runs on it ERROR after the 1 s lock_timeout,
	//    the run finishes, and the holder never waits behind the check.
	holder, err := pgx.Connect(e.ctx, e.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close(context.Background())
	tx, err := holder.Begin(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(e.ctx, "LOCK TABLE "+e.schema+".known_routes IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var locked *planguard.Report
	var runErr error
	t0 := time.Now()
	go func() {
		defer close(done)
		locked, runErr = p.Run(e.ctx)
	}()
	var holderMax time.Duration
	for running := true; running; {
		select {
		case <-done:
			running = false
		case <-time.After(100 * time.Millisecond):
			t1 := time.Now()
			if _, err := tx.Exec(e.ctx, "SELECT count(*) FROM "+e.schema+".known_routes"); err != nil {
				t.Fatal(err)
			}
			holderMax = max(holderMax, time.Since(t1))
		}
	}
	elapsed := time.Since(t0)
	t1 := time.Now()
	if err := tx.Commit(e.ctx); err != nil {
		t.Fatal(err)
	}
	holderMax = max(holderMax, time.Since(t1))
	if runErr != nil {
		t.Fatalf("run under the lock: %v", runErr)
	}
	if holderMax > time.Second {
		t.Errorf("the lock holder waited %v", holderMax)
	}
	got := map[string]planguard.Result{}
	ran := 0
	for _, r := range locked.Results {
		got[r.ID+"|"+r.Mode] = r
		if r.Verdict == planguard.VerdictSkip {
			continue
		}
		ran++
		if r.Verdict != planguard.VerdictError || len(r.Failures) == 0 || !strings.Contains(r.Failures[0].Msg, "55P03") {
			t.Errorf("%s|%s under the lock: %s %v, want ERROR 55P03 (lock_timeout)", r.ID, r.Mode, r.Verdict, r.Failures)
		}
		// A run whose resolver hit the lock is never made (wall 0); every other run waits out lock_timeout once.
		if r.WallMs != 0 && (r.WallMs < 900 || r.WallMs > 2500) {
			t.Errorf("%s|%s waited %.0f ms on the lock, want about 1 s", r.ID, r.Mode, r.WallMs)
		}
	}
	if _, ok := got["routes.delete_expired|custom"]; !ok || ran == 0 {
		t.Fatalf("no DML run under the lock: %v", got)
	}
	// The resolver transaction waits on the lock too (1 s), then each run once.
	if limit := time.Duration(ran+3) * 1500 * time.Millisecond; elapsed > limit {
		t.Errorf("run under the lock took %v for %d runs, want under %v", elapsed, ran, limit)
	}
	if n := e.pooled(t, "SELECT count(*)::text FROM pg_prepared_statements WHERE name LIKE 'planguard%'"); n != "0" {
		t.Errorf("%s planguard prepared statements left after the locked run", n)
	}

	// 4) A holder with SHARE on known_routes blocks only the DML run's ROW EXCLUSIVE. A third session asking for
	//    SHARE is compatible with the holder but queues behind that waiting request, so it waits at most the
	//    check's 1 s lock_timeout.
	third, err := pgx.Connect(e.ctx, e.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close(context.Background())
	if tx, err = holder.Begin(e.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(e.ctx, "LOCK TABLE "+e.schema+".known_routes IN SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	done = make(chan struct{})
	var shared *planguard.Report
	go func() {
		defer close(done)
		shared, runErr = p.Run(e.ctx)
	}()
	var thirdMax time.Duration
	for running := true; running; {
		select {
		case <-done:
			running = false
		case <-time.After(20 * time.Millisecond):
			t1 := time.Now()
			if _, err := third.Exec(e.ctx, "BEGIN; LOCK TABLE "+e.schema+".known_routes IN SHARE MODE; ROLLBACK"); err != nil {
				t.Fatal(err)
			}
			thirdMax = max(thirdMax, time.Since(t1))
		}
	}
	if err := tx.Rollback(e.ctx); err != nil {
		t.Fatal(err)
	}
	if runErr != nil {
		t.Fatalf("run beside the SHARE holder: %v", runErr)
	}
	if r := byKey(shared)["routes.delete_expired|custom"]; r.Verdict != planguard.VerdictError || len(r.Failures) == 0 || !strings.Contains(r.Failures[0].Msg, "55P03") {
		t.Errorf("DML beside the SHARE holder: %s %v, want ERROR 55P03", r.Verdict, r.Failures)
	}
	// It queued (well above a bare LOCK) and was released by the check's lock_timeout.
	if thirdMax < 300*time.Millisecond || thirdMax > 1500*time.Millisecond {
		t.Errorf("the session queued behind the check waited at most %v, want about 1 s or less (and > 300 ms, to show it queued)", thirdMax)
	}
}

// Cancelling a run mid-flight never hands the connection back to the pool with an open transaction or a prepared
// statement: it is closed.
func TestPlanguardProdCancelPostgres(t *testing.T) {
	e := newProdEnv(t)
	p := e.checker(t, "testdata/prodcorpus")
	pid := e.pooled(t, "SELECT pg_backend_pid()::text")
	holder, err := pgx.Connect(e.ctx, e.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close(context.Background())
	tx, err := holder.Begin(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	// SHARE lets the readers through and holds the DML run on its lock, inside its transaction, after PREPARE
	// began: that is where the cancel lands.
	if _, err := tx.Exec(e.ctx, "LOCK TABLE "+e.schema+".known_routes IN SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	// The observer reads pg_stat_activity outside any transaction, so each read is a fresh snapshot.
	observer, err := pgx.Connect(e.ctx, e.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(context.Background())
	ctx, cancel := context.WithCancel(e.ctx)
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		// Wait until the check's connection is waiting on the lock, then cancel.
		for ctx.Err() == nil {
			var n int
			if err := observer.QueryRow(e.ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1::int AND wait_event_type = 'Lock'", pid).Scan(&n); err == nil && n > 0 {
				cancel()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	rep, err := p.Run(ctx)
	cancel()
	<-probeDone
	if err == nil {
		t.Fatalf("run survived its cancel: %d runs", len(rep.Results))
	}
	if got := e.pooled(t, "SELECT pg_backend_pid()::text"); got == pid {
		t.Errorf("the cancelled run's connection (pid %s) went back to the pool", pid)
	}
	if n := e.pooled(t, "SELECT count(*)::text FROM pg_prepared_statements WHERE name LIKE 'planguard%'"); n != "0" {
		t.Errorf("%s planguard prepared statements on the pooled connection", n)
	}
	if ro := e.pooled(t, "SHOW transaction_read_only"); ro != "off" {
		t.Errorf("pooled connection inside a read-only transaction")
	}
	// The closed backend leaves pg_stat_activity once it has processed the terminate.
	alive := 1
	for deadline := time.Now().Add(5 * time.Second); alive != 0 && time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if err := observer.QueryRow(e.ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1::int", pid).Scan(&alive); err != nil {
			t.Fatal(err)
		}
	}
	if alive != 0 {
		t.Errorf("the cancelled run's backend %s is still connected", pid)
	}
}

// checkProdRuns checks a run of testdata/prodcorpus: nothing fails, the null resolver and the pending statement
// skip, the ingest statement (auto only) is not planned, and no run is auto.
func checkProdRuns(t *testing.T, label string, rep *planguard.Report) {
	t.Helper()
	got := map[string]planguard.Result{}
	for _, r := range rep.Results {
		got[r.ID+"|"+r.Mode] = r
		if r.Verdict.Failing() {
			t.Errorf("%s: %s|%s %s %v", label, r.ID, r.Mode, r.Verdict, r.Failures)
		}
		if r.Mode == planguard.ModeAuto || r.ID == "channels.upsert" {
			t.Errorf("%s: %s|%s was planned", label, r.ID, r.Mode)
		}
	}
	for _, k := range []string{"routes.byiata|custom", "routes.byiata|generic", "routes.byiata.empty_region|custom",
		"routes.globalhop|custom", "routes.delete_expired|custom"} {
		if r, ok := got[k]; !ok || r.Verdict == planguard.VerdictSkip {
			t.Errorf("%s: %s did not run: %+v", label, k, r)
		}
	}
	for k, want := range map[string]string{
		"routes.byiata.no_region|custom": "resolver no_region returned no value on this tier",
		"routes.global.pending|custom":   "pending: later",
	} {
		if r := got[k]; r.Verdict != planguard.VerdictSkip || r.Skip != want {
			t.Errorf("%s: %s %s skip %q, want SKIP %q", label, k, r.Verdict, r.Skip, want)
		}
	}
	if len(rep.Results) != 7 {
		t.Errorf("%s: %d runs, want 7", label, len(rep.Results))
	}
}
