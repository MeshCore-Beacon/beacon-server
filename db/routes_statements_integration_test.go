// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Four shapes × cursor forms × limits, both plan modes: rows equal today's statement, in order.
func TestListKnownRoutesMatchesLegacyPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	mustExec(t, ctx, tx, `CREATE INDEX idx_known_routes_hop_iata ON known_routes (hop_count, iata);`+routeFixture)
	s := routeStore(tx)
	tie := time.Date(2026, 1, 10, 0, 0, 0, 123_000_000, time.UTC)
	cursors := []struct {
		at time.Time
		id int64
	}{{}, {tie, 0}, {tie, 13}, {tie.Add(time.Millisecond), 0}, {tie.Add(time.Millisecond), 16}, {time.Date(2026, 1, 9, 23, 58, 0, 0, time.UTC), 0}}
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		mustExec(t, ctx, tx, "SET LOCAL plan_cache_mode = "+mode)
		for _, iata := range []string{"", "YYZ", "GPT", "ZZZ", "yyz", "GPTX", "YYZ ", "   "} {
			for _, hop := range []int32{0, 3, 9} {
				for _, c := range cursors {
					for _, limit := range []int32{1, 2, 50} {
						var cts pgtype.Timestamptz
						if !c.at.IsZero() {
							cts = pgtype.Timestamptz{Time: c.at, Valid: true}
						}
						want := legacyRows[knownRouteRow](t, ctx, tx, legacyListKnownRoutes, iata, hop, cts, limit, c.id)
						got, err := s.listKnownRouteRows(ctx, iata, hop, c.at, c.id, limit)
						if err != nil || !sameRows(got, want) {
							t.Fatalf("%s iata=%q hop=%d cursor=%v/%d limit=%d: got %v, want %v (%v)", mode, iata, hop, c.at, c.id, limit, ids(got), ids(want), err)
						}
					}
				}
			}
		}
	}
}

// A legacy cursor page skips the tied ms as today; cursor+cursorId walks every row once.
func TestKnownRoutesCursorTiesPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	mustExec(t, ctx, tx, `CREATE INDEX idx_known_routes_hop_iata ON known_routes (hop_count, iata);`+routeFixture)
	s := routeStore(tx)
	tie := time.Date(2026, 1, 10, 0, 0, 0, 123_000_000, time.UTC)
	for _, sh := range []struct {
		iata string
		hop  int32
	}{{"", 0}, {"", 3}, {"YYZ", 0}, {"YOW", 3}} {
		want := legacyRows[knownRouteRow](t, ctx, tx, legacyListKnownRoutes, sh.iata, sh.hop, pgtype.Timestamptz{Time: tie, Valid: true}, int32(50), int64(0))
		got, err := s.listKnownRouteRows(ctx, sh.iata, sh.hop, tie, 0, 50)
		if err != nil || !sameRows(got, want) {
			t.Fatalf("%+v cursor without id: got %v want %v", sh, ids(got), ids(want))
		}
		all := legacyRows[knownRouteRow](t, ctx, tx, legacyListKnownRoutes, sh.iata, sh.hop, pgtype.Timestamptz{}, int32(1000), int64(0))
		var walked []knownRouteRow
		cursor, cursorID := time.Time{}, int64(0)
		for range len(all) + 2 {
			page, err := s.listKnownRouteRows(ctx, sh.iata, sh.hop, cursor, cursorID, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) == 0 {
				break
			}
			walked = append(walked, page[0])
			cursor, cursorID = time.UnixMilli(page[0].LastSeen.Time.UnixMilli()), page[0].ID
		}
		if !sameRows(walked, all) {
			t.Fatalf("%+v limit-1 walk: %v, want %v", sh, ids(walked), ids(all))
		}
	}
}

// An unanalyzed region pages from iata_keyset, never a Sort over the region. Built on the
// end-state index set (the baseline less what migrations 004-009 drop), where the (hop_count, iata) index must not win.
func TestKnownRoutesUnanalyzedRegionPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	mustExec(t, ctx, tx, `
DROP INDEX idx_known_routes_hop_count, idx_known_routes_iata_last_seen, idx_known_routes_last_seen, idx_known_routes_reconfirm;
CREATE INDEX idx_known_routes_hop_iata ON known_routes (hop_count, iata);
INSERT INTO iata_codes (iata) VALUES ('NEW');
INSERT INTO known_routes (id,path_key,node_ids,hash_prefix,iata,hop_count,last_seen)
SELECT i, int8send(i), '{}', '{}', (ARRAY['YYZ','YOW','YVR'])[1 + i % 3], 2 + i % 20, '2026-01-01'::timestamptz - i * interval '1 second'
FROM generate_series(1, 60000) i;
ANALYZE known_routes;
INSERT INTO known_routes (id,path_key,node_ids,hash_prefix,iata,hop_count,last_seen)
SELECT 100000 + i, int8send(100000 + i), '{}', '{}', 'NEW', 2 + i % 20, '2026-01-02'::timestamptz - i * interval '1 second'
FROM generate_series(1, 20000) i;`)
	capture := &statementCapture{Tx: tx, name: "ListKnownRoutesByIATA"}
	s := &Store{q: sqlc.New(unnamedDBTX{capture})}
	if _, err := s.listKnownRouteRows(ctx, "NEW", 0, time.Time{}, 0, 50); err != nil {
		t.Fatal(err)
	}
	plan := explainJSON(t, ctx, tx, capture.statement, capture.args...)
	if !strings.Contains(plan, `"Index Name": "idx_known_routes_iata_keyset"`) || strings.Contains(plan, `"Node Type": "Sort"`) ||
		strings.Contains(plan, `"Node Type": "Incremental Sort"`) || sharedBuffers(t, plan) > 300 {
		t.Fatalf("unanalyzed region plan:\n%s", plan)
	}
}

// xactReads is the known_routes tuples this transaction has fetched by index scans or read by seq scans.
// Postgres has no per-transaction index view, so a scan that filters hop_count shows here as heap fetches.
const xactReads = `SELECT coalesce(seq_tup_read, 0) + coalesce(idx_tup_fetch, 0) FROM pg_stat_xact_user_tables WHERE relid = 'known_routes'::regclass`

// The hop pair never runs as a named prepared statement: the pg_prepared_statements check is the
// guard. The tuple count on the 7th call of an absent hop is only a coarse sanity check; it does
// not pin the plan.
func TestKnownRoutesHopStatementsStayCustomPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	mustExec(t, ctx, tx, `CREATE INDEX idx_known_routes_hop_iata ON known_routes (hop_count, iata);
INSERT INTO known_routes (id,path_key,node_ids,hash_prefix,iata,hop_count,last_seen)
SELECT i, int8send(i), '{}', '{}', (ARRAY['YYZ','YOW'])[1 + i % 2], 2 + i % 20, '2026-01-01'::timestamptz - i * interval '1 second'
FROM generate_series(1, 50000) i;
ANALYZE known_routes;`)
	s := routeStore(tx)
	for _, c := range []struct{ iata, statement string }{{"", "ListKnownRoutesGlobalHop"}, {"YYZ", "ListKnownRoutesByIATAHop"}} {
		for range 6 {
			if _, err := s.listKnownRouteRows(ctx, c.iata, 1, time.Time{}, 0, 50); err != nil {
				t.Fatal(err)
			}
		}
		before := countRows(t, ctx, tx, xactReads)
		if _, err := s.listKnownRouteRows(ctx, c.iata, 1, time.Time{}, 0, 50); err != nil {
			t.Fatal(err)
		}
		after := countRows(t, ctx, tx, xactReads)
		// This count is itself a named statement holding the name, so it leaves itself out.
		if n := countRows(t, ctx, tx, `SELECT count(*) FROM pg_prepared_statements WHERE statement LIKE '%`+c.statement+`%' AND statement NOT LIKE '%pg_prepared_statements%'`); n != 0 {
			t.Fatalf("%d named prepared statements for %s", n, c.statement)
		}
		if after-before > 10 {
			t.Fatalf("7th absent-hop %s page read %d tuples; want ≤ 10", c.statement, after-before)
		}
	}
}

// The global hop page picks its branch by the statistics' estimate of the hop group: a group estimated under the bound
// reads and sorts the group (kh), any other runs today's statement (kk), and so does every group before the table has
// statistics. Both return today's rows, in order, on every page of a cursor walk, with rows tied on the millisecond.
func TestGlobalHopBranchesMatchTodayPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// 3000 routes: hop 3 holds 2400 (over the 1000 floor of the bound), hop 7 500, hop 9 60; three routes share each ms.
	if _, err := pool.Exec(ctx, `
INSERT INTO iata_codes (iata) VALUES ('YYZ'), ('YOW');
INSERT INTO known_routes (id, path_key, node_ids, hash_prefix, iata, hop_count, last_seen)
SELECT i, int8send(i), '{}', '{}', (ARRAY['YYZ','YOW'])[1 + i % 2],
       CASE WHEN i % 50 < 40 THEN 3 WHEN i % 50 < 49 THEN 7 ELSE 9 END,
       '2026-01-10'::timestamptz - (i / 3) * interval '1 millisecond' - (i % 3) * interval '100 microseconds'
FROM generate_series(1, 3000) i;`); err != nil {
		t.Fatal(err)
	}
	branchRan := func(tx pgx.Tx, hop int32) (kh, kk int) {
		t.Helper()
		capture := &statementCapture{Tx: tx, name: "ListKnownRoutesGlobalHop"}
		cs := &Store{q: sqlc.New(unnamedDBTX{capture})}
		if _, err := cs.listKnownRouteRows(ctx, "", hop, time.Time{}, 0, 50); err != nil {
			t.Fatal(err)
		}
		plan := explainJSON(t, ctx, tx, capture.statement, capture.args...)
		return aliasLoops(t, plan, "kh"), aliasLoops(t, plan, "kk")
	}
	// No statistics yet: today's statement, even for the small group.
	notx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if kh, kk := branchRan(notx, 7); kh != 0 || kk != 1 {
		t.Errorf("unanalyzed hop 7: kh ran %d, kk %d times; want 0 and 1", kh, kk)
	}
	_ = notx.Rollback(ctx)
	if _, err := pool.Exec(ctx, "ANALYZE known_routes"); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	s := routeStore(tx)
	for _, hop := range []int32{3, 7, 9, 12} {
		all := legacyRows[knownRouteRow](t, ctx, tx, legacyListKnownRoutes, "", hop, pgtype.Timestamptz{}, int32(5000), int64(0))
		for _, limit := range []int32{1, 7, 50, 200} {
			// cursor + cursorId: the pages together are every row once, in order.
			var walked []knownRouteRow
			cursor, cursorID := time.Time{}, int64(0)
			for range len(all)/int(limit) + 2 {
				page, err := s.listKnownRouteRows(ctx, "", hop, cursor, cursorID, limit)
				if err != nil {
					t.Fatal(err)
				}
				var cts pgtype.Timestamptz
				if !cursor.IsZero() {
					cts = pgtype.Timestamptz{Time: cursor, Valid: true}
				}
				if want := legacyRows[knownRouteRow](t, ctx, tx, legacyListKnownRoutes, "", hop, cts, limit, cursorID); !sameRows(page, want) {
					t.Fatalf("hop %d limit %d cursor %v/%d: got %v, want %v", hop, limit, cursor, cursorID, ids(page), ids(want))
				}
				if len(page) == 0 {
					break
				}
				walked = append(walked, page...)
				last := page[len(page)-1]
				cursor, cursorID = time.UnixMilli(last.LastSeen.Time.UnixMilli()), last.ID
			}
			if !sameRows(walked, all) {
				t.Fatalf("hop %d limit %d walk: %d rows, want %d", hop, limit, len(walked), len(all))
			}
			// A cursor without an id skips the rest of its millisecond, as today.
			if len(all) > 4 {
				at := time.UnixMilli(all[4].LastSeen.Time.UnixMilli())
				want := legacyRows[knownRouteRow](t, ctx, tx, legacyListKnownRoutes, "", hop, pgtype.Timestamptz{Time: at, Valid: true}, limit, int64(0))
				got, err := s.listKnownRouteRows(ctx, "", hop, at, 0, limit)
				if err != nil || !sameRows(got, want) {
					t.Fatalf("hop %d limit %d cursor without id: got %v, want %v (%v)", hop, limit, ids(got), ids(want), err)
				}
			}
		}
	}

	// Which branch ran: hop 3 (estimated over the bound) runs today's statement (kk), hop 7 reads its group (kh).
	if kh, kk := branchRan(tx, 3); kh != 0 || kk != 1 {
		t.Errorf("hop 3: kh ran %d, kk %d times; want 0 and 1", kh, kk)
	}
	if kh, kk := branchRan(tx, 7); kh != 1 || kk != 0 {
		t.Errorf("hop 7: kh ran %d, kk %d times; want 1 and 0", kh, kk)
	}
}

// aliasLoops returns the Actual Loops of the plan node scanning alias, -1 if there is none.
func aliasLoops(t *testing.T, plan, alias string) int {
	t.Helper()
	var root []struct{ Plan map[string]any }
	if err := json.Unmarshal([]byte(plan), &root); err != nil || len(root) != 1 {
		t.Fatalf("plan json: %v", err)
	}
	var find func(n map[string]any) int
	find = func(n map[string]any) int {
		if n["Alias"] == alias {
			return int(n["Actual Loops"].(float64))
		}
		kids, _ := n["Plans"].([]any)
		for _, k := range kids {
			if l := find(k.(map[string]any)); l >= 0 {
				return l
			}
		}
		return -1
	}
	return find(root[0].Plan)
}
