// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
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
	s := &Store{q: sqlc.New(capture), qc: sqlc.New(unnamedDBTX{capture})}
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
