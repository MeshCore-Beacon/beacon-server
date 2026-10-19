// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	pgdata "github.com/MeshCore-Beacon/beacon-server/db/planguard"
	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const diffCases = `batches:
  - name: snap
    search_path: "SCHEMA"
    statements:
      old: "old:ListT"
      new: "new:ListT"
      ident: "file:ident.sql"
      page: "file:page.sql"
      oracle: "file:page.sql"
    pairs:
      - id: tie
        ref: {stmt: old, params: []}
        cands: [{stmt: new, params: []}]
        key: [k]
        allowed: [tie_order]
        expect: {new: allowed}
      - id: ident
        ref: {stmt: ident, params: []}
        cands: [{stmt: ident, params: []}]
        expect: {ident: identical}
      - id: walk
        ref: {stmt: oracle, params: [2147483647, 2147483647, 8]}
        cands: [{stmt: page, params: [2147483647, 2147483647, 8]}]
        walk:
          limit_param: 3
          fetch_extra: 1
          cursor: [{param: 1, column: k}, {param: 2, column: id}]
          first: [2147483647, 2147483647]
          web_limit: 50
          oracle: true
        expect: {page: identical}
      - id: resolved
        ref: {stmt: page, params: [{sql: "SELECT max(k) FROM t"}, 2147483647, 5]}
        cands: [{stmt: oracle, params: [{sql: "SELECT max(k) FROM t"}, 2147483647, 5]}]
        expect: {oracle: identical}
      - id: novalue
        ref: {stmt: page, params: [{sql: "SELECT k FROM t WHERE false"}, 2147483647, 5]}
        cands: [{stmt: oracle, params: [30, 2147483647, 5]}]
`

const writeCases = `batches:
  - name: write
    search_path: "SCHEMA"
    statements:
      w: "file:write.sql"
    pairs:
      - id: w
        ref: {stmt: w, params: []}
        cands: [{stmt: w, params: []}]
`

func TestRunDiffSnapshotPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "planguard_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close(context.Background())
	})
	if _, err := admin.Exec(ctx, `
CREATE SCHEMA `+schema+`;
CREATE TABLE `+schema+`.t (id int PRIMARY KEY, k int NOT NULL, v text);
INSERT INTO `+schema+`.t SELECT g, g / 10, CASE WHEN g % 7 = 0 THEN NULL ELSE 'v' || g || E'\n"q",' END
FROM generate_series(1, 300) g;
CREATE FUNCTION `+schema+`.write_row() RETURNS int LANGUAGE sql AS
  'INSERT INTO `+schema+`.t VALUES (1000, 1000, ''written'') RETURNING id';`); err != nil {
		t.Fatal(err)
	}
	pc, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close(context.Background())
	sess := planguard.NewSession(pc)
	scalar := func(sql string) string {
		t.Helper()
		res, err := pc.Exec(ctx, sql).ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		return string(res[len(res)-1].Rows[0][0])
	}
	files := mapFS(map[string]string{
		"cases.yaml": strings.ReplaceAll(diffCases, "SCHEMA", schema),
		"write.yaml": strings.ReplaceAll(writeCases, "SCHEMA", schema),
		"ident.sql":  "SELECT current_setting('transaction_isolation') AS iso, current_setting('transaction_read_only') AS ro;",
		"page.sql":   "SELECT id, k, v FROM t WHERE (k, id) < ($1, $2) ORDER BY k DESC, id DESC LIMIT $3",
		"write.sql":  "SELECT write_row() AS id",
	})
	oldSQL := map[string]string{"ListT": "-- name: ListT :many\nSELECT id, k, v FROM t ORDER BY k, id"}
	newSQL := map[string]string{"ListT": "-- name: ListT :many\nSELECT id, k, v FROM t ORDER BY k, id DESC;"}

	// A statement that writes fails in the READ ONLY transaction; nothing is written and nothing stays prepared.
	wspec, err := planguard.LoadDiffSpec(files, "write.yaml")
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if _, err := planguard.RunDiff(ctx, sess, wspec, oldSQL, newSQL, files, out, nil); err == nil || !strings.Contains(err.Error(), "25006") {
		t.Errorf("writing alias: err %v, want a 25006 read-only error", err)
	}
	if n := scalar("SELECT count(*) FROM " + schema + ".t"); n != "300" {
		t.Errorf("%s rows after the writing alias, want 300", n)
	}
	if n := scalar("SELECT count(*) FROM pg_prepared_statements"); n != "0" {
		t.Errorf("pg_prepared_statements = %s after a failed batch, want 0", n)
	}

	spec, err := planguard.LoadDiffSpec(files, "cases.yaml")
	if err != nil {
		t.Fatal(err)
	}
	rep, err := planguard.RunDiff(ctx, sess, spec, oldSQL, newSQL, files, out, []int{7, 0})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]planguard.PairResult{}
	for _, b := range rep.Batches {
		for _, p := range b.Pairs {
			got[p.Pair+"|"+p.Cand] = p
		}
	}
	tie := got["tie|new"]
	if tie.Verdict != "allowed" || len(tie.Outcome.Classes) != 1 || !strings.HasPrefix(tie.Outcome.Classes[0], "tie_order(") || !tie.OK {
		t.Errorf("tie pair: %+v, want allowed (tie_order)", tie)
	}
	if tie.RefRows != 300 || tie.CandRows != 300 {
		t.Errorf("tie pair rows %d/%d, want 300/300", tie.RefRows, tie.CandRows)
	}
	if id := got["ident|ident"]; id.Verdict != "identical" || !id.OK {
		t.Errorf("ident pair: %+v", id)
	}
	walk := got["walk|page"]
	if walk.Verdict != "identical" || !walk.OK || len(walk.Walks) != 2 {
		t.Fatalf("walk pair: %+v", walk)
	}
	for _, w := range walk.Walks {
		if w.Outcome.Rows != 300 || w.RefRows != 300 || !w.Outcome.Clean() {
			t.Errorf("walk at limit %d: %+v (ref %d rows)", w.Limit, w.Outcome, w.RefRows)
		}
	}
	if walk.Walks[1].Limit != 50 {
		t.Errorf("web limit %d, want 50", walk.Walks[1].Limit)
	}
	if len(rep.Failed()) != 0 {
		t.Errorf("failed pairs: %+v", rep.Failed())
	}
	// {sql: …} params are read in the snapshot; one that finds no value skips its pair.
	if r := got["resolved|oracle"]; r.Verdict != "identical" || r.RefRows != 5 || !r.OK {
		t.Errorf("resolved pair: %+v", r)
	}
	if r := got["novalue|oracle"]; r.Verdict != planguard.DiffSkip || r.OK || len(rep.Skipped()) != 1 {
		t.Errorf("pair whose resolver finds nothing: %+v (skipped %d), want one skip", r, len(rep.Skipped()))
	}

	// One case file per (pair, role, alias), read back for the comparison.
	for _, f := range []string{"tie.ref.old", "tie.cand.new", "ident.ref.ident", "ident.cand.ident", "walk.ref.oracle", "walk.cand.page",
		"walk.walk7.ref.oracle", "walk.walk7.cand.page", "walk.walk50.ref.oracle", "walk.walk50.cand.page"} {
		if _, err := os.Stat(filepath.Join(out, "snap", f+".jsonl")); err != nil {
			t.Errorf("case file %s: %v", f, err)
		}
	}
	ident, err := planguard.ReadCaseFile(filepath.Join(out, "snap", "ident.ref.ident.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(ident.Rows) != 1 || *ident.Rows[0][0] != "repeatable read" || *ident.Rows[0][1] != "on" {
		t.Errorf("transaction isolation/read_only %v, want repeatable read / on", ident.Rows)
	}
	tieRef, err := planguard.ReadCaseFile(filepath.Join(out, "snap", "tie.ref.old.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if v := tieRef.Rows[1][2]; v == nil || *v != "v2\n\"q\"," {
		t.Errorf("row 2 v = %v, want the text with its newline, quotes and comma", v)
	}
	if tieRef.Rows[6][2] != nil {
		t.Errorf("row 7 v = %q, want NULL", *tieRef.Rows[6][2])
	}
	if _, err := os.Stat(filepath.Join(out, "diff_report.md")); err != nil {
		t.Error(err)
	}
	if n := scalar("SELECT count(*) FROM pg_prepared_statements"); n != "0" {
		t.Errorf("pg_prepared_statements = %s after RunDiff, want 0", n)
	}
	if st := pc.TxStatus(); st != 'I' {
		t.Errorf("transaction status %q after RunDiff, want idle", st)
	}

	// A pair that misses its expect is reported as failed.
	spec.Batches[0].Pairs[0].Expect = map[string]string{"new": "identical"}
	rep, err = planguard.RunDiff(ctx, sess, spec, oldSQL, newSQL, files, out, nil)
	if err != nil {
		t.Fatal(err)
	}
	if f := rep.Failed(); len(f) != 1 || f[0].Pair != "tie" {
		t.Errorf("failed pairs %+v, want the tie pair", f)
	}
}

// The planted readmodels case on PostgreSQL: the frozen statement texts, walked over the in-memory walk test's
// data. The readmodels statement skips site B's next packet at limit 1 and is right at limit 7; the end-state
// statement matches the oracle at both.
func TestRunDiffFindsReadmodelsSkipPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
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
	defer pool.Close()
	if err := db.RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	site := map[string]string{"A": "AAA", "B": "BBB", "C": "CCC"}
	var hashes, iatas []string
	var ats []int
	for _, r := range walkData() {
		hashes, iatas, ats = append(hashes, r.hash), append(iatas, site[r.site]), append(ats, r.at)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO iata_codes (iata) VALUES ('AAA'), ('BBB'), ('CCC');
INSERT INTO observers (id, public_key) VALUES ('00000000-0000-0000-0000-000000000001', '\x01'),
  ('00000000-0000-0000-0000-000000000002', '\x02'), ('00000000-0000-0000-0000-000000000003', '\x03');`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
WITH o AS (
  SELECT decode(h, 'hex') AS h, i::bpchar AS iata, timestamptz '2026-10-18 00:00:00+00' + a * interval '1 second' AS at
  FROM unnest($1::text[], $2::text[], $3::int[]) AS u(h, i, a)),
p AS (
  INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header,
                       first_heard_at, last_heard_at, observation_count)
  SELECT h, 4, 1, 1, '\x00', '\x00', min(at), max(at), count(*) FROM o GROUP BY h
  RETURNING packet_hash)
INSERT INTO packet_observations (packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count)
SELECT o.h, ('00000000-0000-0000-0000-00000000000' || (ascii(o.iata) - 64))::uuid, o.iata, o.at, 0, 1, 0
FROM o JOIN p ON p.packet_hash = o.h`, hashes, iatas, ats); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
CREATE TABLE packet_sites AS
SELECT po.packet_hash, po.iata, max(po.heard_at) AS site_heard_at, p.payload_type, p.route_type, p.scope_id
FROM packet_observations po JOIN packets p USING (packet_hash)
GROUP BY po.packet_hash, po.iata, p.payload_type, p.route_type, p.scope_id;
CREATE INDEX ON packet_sites (iata, site_heard_at DESC, packet_hash DESC);
ANALYZE packet_sites;`); err != nil {
		t.Fatal(err)
	}

	spec, err := planguard.ParseDiffSpec([]byte(strings.ReplaceAll(`batches:
  - name: planted
    search_path: "SCHEMA"
    statements:
      rm: "file:sql/planted/ListPacketsBySitesReadmodels.sql"
      end: "file:sql/pending/ListPacketsBySites.sql"
      oracle: "file:sql/oracle/OracleBySites.sql"
    pairs:
      - id: walk
        ref: {stmt: oracle, params: ["{AAA,BBB,CCC}", infinity, '\x', 2]}
        cands:
          - {stmt: rm, params: ["{AAA,BBB,CCC}", infinity, '\x', 2]}
          - {stmt: end, params: ["{AAA,BBB,CCC}", infinity, '\x', 2]}
        key: [site_heard_at]
        walk:
          limit_param: 4
          fetch_extra: 1
          cursor: [{param: 2, column: site_heard_at}, {param: 3, column: packet_hash}]
          first: [infinity, '\x']
          oracle: true
        expect: {rm: different, end: identical}
`, "SCHEMA", schema)), "planted.yaml", pgdata.Files)
	if err != nil {
		t.Fatal(err)
	}
	pc, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close(context.Background())
	rep, err := planguard.RunDiff(ctx, planguard.NewSession(pc), spec, nil, nil, pgdata.Files, t.TempDir(), []int{1, 2, 3, 7})
	if err != nil {
		t.Fatal(err)
	}
	if f := rep.Failed(); len(f) != 0 {
		t.Errorf("failed pairs: %+v", f)
	}
	for _, p := range rep.Batches[0].Pairs {
		if len(p.Walks) != 4 {
			t.Fatalf("%s: %d walks, want 4", p.Cand, len(p.Walks))
		}
		for _, w := range p.Walks {
			o := w.Outcome
			switch {
			case p.Cand == "rm" && w.Limit == 1:
				if o.FirstWrong != 3 || o.Missing == 0 {
					t.Errorf("rm at limit 1: %+v, want the first wrong row at 3 and rows missing", o)
				}
			case !o.Clean() || o.Rows != 40:
				t.Errorf("%s at limit %d: %+v, want clean over 40 rows", p.Cand, w.Limit, o)
			}
		}
	}
}
