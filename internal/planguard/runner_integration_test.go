// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// runnerEnv is a migrated schema holding ~2,000 known_routes over three regions, and a pgconn session on it.
type runnerEnv struct {
	ctx    context.Context
	schema string
	pool   *pgxpool.Pool
	pc     *pgconn.PgConn
	corpus *planguard.Corpus
}

func newRunnerEnv(t *testing.T) *runnerEnv {
	t.Helper()
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO iata_codes (iata) VALUES ('SEA'), ('YVR'), ('PDX');
INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count, last_seen, observation_count)
SELECT int8send(g), ARRAY[md5(g::text)::uuid], ARRAY['\x01'::bytea], (ARRAY['SEA','YVR','PDX'])[1 + g % 3],
       1 + g % 5, timestamptz '2026-03-01 00:00:00+00' - g * interval '1 minute', 1 + g % 4
FROM generate_series(1, 2000) g`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ANALYZE known_routes"); err != nil {
		t.Fatal(err)
	}
	pc, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close(context.Background()) })
	sqlc, err := planguard.LoadStatements("../../db/sqlc")
	if err != nil {
		t.Fatal(err)
	}
	c, err := planguard.LoadCorpus(os.DirFS("testdata/corpus"), "runner.yaml", sqlc)
	if err != nil {
		t.Fatal(err)
	}
	return &runnerEnv{ctx: ctx, schema: schema, pool: pool, pc: pc, corpus: c}
}

func (e *runnerEnv) run(t *testing.T, only string) *planguard.Report {
	t.Helper()
	rep, err := planguard.Run(e.ctx, e.corpus, planguard.NewSession(e.pc), planguard.RunOptions{
		Tier:       "local",
		Only:       regexp.MustCompile(only),
		SearchPath: e.schema,
		Baseline:   planguard.BaselineOff,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// scalar runs one single-value query on the runner's own connection.
func (e *runnerEnv) scalar(t *testing.T, sql string) string {
	t.Helper()
	res, err := e.pc.Exec(e.ctx, sql).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return string(res[len(res)-1].Rows[0][0])
}

func byKey(rep *planguard.Report) map[string]planguard.Result {
	m := map[string]planguard.Result{}
	for _, r := range rep.Results {
		m[r.ID+"|"+r.Mode] = r
	}
	return m
}

func TestRunnerReadOnlyPostgres(t *testing.T) {
	e := newRunnerEnv(t)
	cacheMode := e.scalar(t, "SHOW plan_cache_mode")
	var before int
	if err := e.pool.QueryRow(e.ctx, "SELECT count(*) FROM known_routes").Scan(&before); err != nil {
		t.Fatal(err)
	}
	rep := e.run(t, `^routes\.(byiata|globalhop|delete_expired)$`)
	got := byKey(rep)
	for _, k := range []string{"routes.byiata|custom", "routes.byiata|generic", "routes.globalhop|custom", "routes.delete_expired|custom"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("no result for %s; results %v", k, got)
		}
	}
	if len(rep.Results) != 4 {
		t.Errorf("results = %d, want 4", len(rep.Results))
	}
	for k, r := range got {
		if r.Verdict.Failing() {
			t.Errorf("%s: verdict %s, failures %v", k, r.Verdict, r.Failures)
		}
		if r.Stmt == "DeleteExpiredRoutes" {
			if r.Buffers != 0 || r.ExecMs != 0 {
				t.Errorf("%s: dml was analyzed: buffers %d exec %.2f ms", k, r.Buffers, r.ExecMs)
			}
			if len(r.Plan) == 0 {
				t.Errorf("%s: no plan", k)
			}
			continue
		}
		if r.Buffers <= 0 || r.PlanningMs <= 0 {
			t.Errorf("%s: buffers %d planning %.3f ms, want both > 0 (EXPLAIN ANALYZE, BUFFERS)", k, r.Buffers, r.PlanningMs)
		}
	}
	if len(rep.Indexes) == 0 || !contains(rep.Indexes, "idx_known_routes_iata_keyset") {
		t.Errorf("indexes = %v, want the known_routes indexes", rep.Indexes)
	}
	if rep.Settings["random_page_cost"] == "" {
		t.Errorf("settings = %v, want random_page_cost", rep.Settings)
	}
	var after int
	if err := e.pool.QueryRow(e.ctx, "SELECT count(*) FROM known_routes").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Errorf("known_routes rows %d → %d; the runner wrote", before, after)
	}
	if n := e.scalar(t, "SELECT count(*) FROM pg_prepared_statements"); n != "0" {
		t.Errorf("pg_prepared_statements = %s after Run, want 0", n)
	}
	if m := e.scalar(t, "SHOW plan_cache_mode"); m != cacheMode {
		t.Errorf("plan_cache_mode = %s after Run, want the session default %s", m, cacheMode)
	}
	if st := e.pc.TxStatus(); st != 'I' {
		t.Errorf("transaction status %q after Run, want idle", st)
	}
}

func TestRunnerRecoversAfterErrorPostgres(t *testing.T) {
	e := newRunnerEnv(t)
	rep := e.run(t, `^(sleep\.timeout|routes\.globalhop)$`)
	got := byKey(rep)
	sl, ok := got["sleep.timeout|custom"]
	if !ok {
		t.Fatalf("no sleep result: %v", got)
	}
	if sl.Verdict != planguard.VerdictError || len(sl.Failures) == 0 || !strings.Contains(sl.Failures[0].Msg, "statement timeout") {
		t.Errorf("sleep: verdict %s failures %v, want ERROR on a statement timeout", sl.Verdict, sl.Failures)
	}
	gh := got["routes.globalhop|custom"]
	if gh.Verdict != planguard.VerdictOK || gh.Buffers <= 0 {
		t.Errorf("globalhop after the timeout: verdict %s buffers %d failures %v, want ok", gh.Verdict, gh.Buffers, gh.Failures)
	}
	if n := e.scalar(t, "SELECT count(*) FROM pg_prepared_statements"); n != "0" {
		t.Errorf("pg_prepared_statements = %s after Run, want 0", n)
	}
	if rep.Counts()[planguard.VerdictError] != 1 || len(rep.Failures()) != 1 {
		t.Errorf("counts %v failures %d, want one ERROR", rep.Counts(), len(rep.Failures()))
	}
}

func TestUnresolvedParamsSkipPostgres(t *testing.T) {
	e := newRunnerEnv(t)
	rep := e.run(t, `^routes\.byiata\.(no_region|busy)$`)
	got := byKey(rep)
	nr := got["routes.byiata.no_region|custom"]
	if nr.Verdict != planguard.VerdictSkip || nr.Skip != "resolver no_region returned no value on this tier" {
		t.Errorf("no_region: verdict %s skip %q", nr.Verdict, nr.Skip)
	}
	busy := got["routes.byiata.busy|custom"]
	if busy.Verdict != planguard.VerdictOK || len(busy.Params) == 0 || busy.Params[0] != "'PDX'" && busy.Params[0] != "'SEA'" && busy.Params[0] != "'YVR'" {
		t.Errorf("busy: verdict %s params %v failures %v", busy.Verdict, busy.Params, busy.Failures)
	}
	for v, n := range rep.Counts() {
		if v.Failing() && n > 0 {
			t.Errorf("counts %v hold a failing verdict", rep.Counts())
		}
	}

	// A resolver that errors fails only its own entries.
	rep = e.run(t, `^routes\.byiata\.(broken|busy)$`)
	got = byKey(rep)
	if br := got["routes.byiata.broken|custom"]; br.Verdict != planguard.VerdictError || len(br.Failures) == 0 || !strings.Contains(br.Failures[0].Msg, "resolver broken") {
		t.Errorf("broken: verdict %s failures %v, want ERROR naming the resolver", br.Verdict, br.Failures)
	}
	if busy := got["routes.byiata.busy|custom"]; busy.Verdict != planguard.VerdictOK {
		t.Errorf("busy beside a broken resolver: verdict %s failures %v", busy.Verdict, busy.Failures)
	}
	if n := e.scalar(t, "SELECT count(*) FROM pg_prepared_statements"); n != "0" {
		t.Errorf("pg_prepared_statements = %s after Run, want 0", n)
	}
}

func TestGenericModeShowsParamsPostgres(t *testing.T) {
	e := newRunnerEnv(t)
	got := byKey(e.run(t, `^routes\.byiata$`))
	conds := func(r planguard.Result) string {
		var out []string
		var rec func(v any)
		rec = func(v any) {
			switch x := v.(type) {
			case map[string]any:
				for k, y := range x {
					if k == "Index Cond" {
						out = append(out, y.(string))
					}
					rec(y)
				}
			case []any:
				for _, y := range x {
					rec(y)
				}
			}
		}
		var doc any
		if err := json.Unmarshal(r.Plan, &doc); err != nil {
			t.Fatalf("%s|%s plan: %v", r.ID, r.Mode, err)
		}
		rec(doc)
		return strings.Join(out, " && ")
	}
	if c := conds(got["routes.byiata|generic"]); !strings.Contains(c, "$1") {
		t.Errorf("generic Index Cond %q, want $1", c)
	}
	if c := conds(got["routes.byiata|custom"]); !strings.Contains(c, "'SEA'::bpchar") || strings.Contains(c, "$1") {
		t.Errorf("custom Index Cond %q, want 'SEA'::bpchar", c)
	}
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}
