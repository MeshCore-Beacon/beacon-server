// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

// routesIATAsFixture adds GPT routes to routeFixture's 0.123 s tie (ids above the tie's, so GPT leads it) and one
// to its 0.124 s tie, so ties cross IATAs.
const routesIATAsFixture = `
INSERT INTO known_routes (id,path_key,node_ids,hash_prefix,iata,hop_count,first_seen,last_seen)
SELECT 500 + i, int8send(500 + i), ARRAY[md5('t' || i)::uuid], ARRAY['\x03'::bytea], 'GPT', 2 + i % 4, '2026-01-01',
       CASE WHEN i = 4 THEN '2026-01-10 00:00:00.124'::timestamptz
            ELSE '2026-01-10 00:00:00.123'::timestamptz + i * interval '150 microseconds' END
FROM generate_series(1, 4) i;`

// A region page is the merge of its IATAs' own pages: the same rows in the same order, on every page of a keyset
// walk and for cursor-only pages, ties inside a millisecond included, under both plan modes.
func TestKnownRoutesByIATAsMatchMergePostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	mustExec(t, ctx, tx, `CREATE INDEX idx_known_routes_hop_iata ON known_routes (hop_count, iata);`+routeFixture+routesIATAsFixture)
	s := routeStore(tx)
	newest := func(a, b knownRouteRow) int {
		return cmp.Or(cmp.Compare(b.LastSeen.Time.UnixMilli(), a.LastSeen.Time.UnixMilli()), cmp.Compare(b.ID, a.ID))
	}
	tie := time.Date(2026, 1, 10, 0, 0, 0, 123_000_000, time.UTC)
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		mustExec(t, ctx, tx, "SET LOCAL plan_cache_mode = "+mode)
		for _, iatas := range [][]string{{"YYZ", "YOW"}, {"GPT", "YYZ", "YOW", "YVR"}, {"GPT", "YOW", "ZZZ"}, {"YVR", "yyz", "GPT"}, {"YOW", "GPT", "YOW"}} {
			for _, hop := range []int32{0, 3, 9} {
				var all []knownRouteRow
				for _, iata := range slices.Compact(slices.Sorted(slices.Values(iatas))) {
					rows, err := s.listKnownRouteRows(ctx, iata, hop, time.Time{}, 0, 1000)
					if err != nil {
						t.Fatal(err)
					}
					all = append(all, rows...)
				}
				slices.SortFunc(all, newest)
				name := fmt.Sprintf("%s %v hop %d", mode, iatas, hop)
				if hop != 9 && len(all) < 20 {
					t.Fatalf("%s: only %d routes to merge", name, len(all))
				}

				for _, limit := range []int32{1, 2, 7, 50} {
					var walked []knownRouteRow
					cursor, cursorID := time.Time{}, int64(0)
					for range len(all) + 2 {
						page, err := s.listIATAsRouteRows(ctx, iatas, hop, cursor, cursorID, limit)
						if err != nil {
							t.Fatal(err)
						}
						if want := min(int(limit), len(all)-len(walked)); len(page) != want {
							t.Fatalf("%s limit %d: page after %d rows holds %d, want %d", name, limit, len(walked), len(page), want)
						}
						if len(page) == 0 {
							break
						}
						walked = append(walked, page...)
						last := page[len(page)-1]
						cursor, cursorID = time.UnixMilli(last.LastSeen.Time.UnixMilli()), last.ID
					}
					if !sameRows(walked, all) {
						t.Fatalf("%s limit %d walk: %v, want %v", name, limit, ids(walked), ids(all))
					}
				}

				// Cursor-only pages skip their whole millisecond; with an id they resume inside it.
				for _, c := range []struct {
					at time.Time
					id int64
				}{{tie, 0}, {tie, 15}, {tie, 502}, {tie.Add(time.Millisecond), 0}, {tie.Add(time.Millisecond), 504}, {time.Date(2026, 1, 9, 23, 58, 0, 0, time.UTC), 0}} {
					want := []knownRouteRow{}
					for _, r := range all {
						ms := r.LastSeen.Time.UnixMilli()
						if ms < c.at.UnixMilli() || (c.id != 0 && ms == c.at.UnixMilli() && r.ID < c.id) {
							want = append(want, r)
						}
					}
					want = want[:min(len(want), 7)]
					got, err := s.listIATAsRouteRows(ctx, iatas, hop, c.at, c.id, 7)
					if err != nil || !sameRows(got, want) {
						t.Fatalf("%s cursor %v/%d: got %v, want %v (%v)", name, c.at, c.id, ids(got), ids(want), err)
					}
				}
			}
		}
	}

	// The API page carries the same routes, resolved.
	routes, err := s.ListKnownRoutes(ctx, []string{"GPT", "YOW"}, 0, tie.Add(time.Millisecond), 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := s.listIATAsRouteRows(ctx, []string{"GPT", "YOW"}, 0, tie.Add(time.Millisecond), 0, 50)
	if err != nil || len(routes) != len(rows) || len(rows) == 0 {
		t.Fatalf("%d routes for %d rows (%v)", len(routes), len(rows), err)
	}
	for i, r := range routes {
		if r.ID != rows[i].ID || r.IATA != rows[i].Iata || r.LastSeen != rows[i].LastSeen.Time.UnixMilli() {
			t.Fatalf("route %d: %+v, row %d", i, r, rows[i].ID)
		}
	}
}

// A region page reads each listed IATA's own page, never the global keyset: sparse IATAs behind 60,000 newer routes
// of another IATA cost no more than a page each.
func TestKnownRoutesByIATAsBoundedPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	mustExec(t, ctx, tx, `
DROP INDEX idx_known_routes_hop_count, idx_known_routes_iata_last_seen, idx_known_routes_last_seen, idx_known_routes_reconfirm;
CREATE INDEX idx_known_routes_hop_iata ON known_routes (hop_count, iata);
INSERT INTO iata_codes (iata) VALUES ('GPT');
INSERT INTO known_routes (id,path_key,node_ids,hash_prefix,iata,hop_count,last_seen)
SELECT i, int8send(i), '{}', '{}', 'YYZ', 2 + i % 20, '2026-01-02'::timestamptz - i * interval '1 second'
FROM generate_series(1, 60000) i;
INSERT INTO known_routes (id,path_key,node_ids,hash_prefix,iata,hop_count,last_seen)
SELECT 100000 + i, int8send(100000 + i), '{}', '{}', (ARRAY['GPT','YOW','YVR'])[1 + i % 3], 2 + i % 4,
       '2026-01-01'::timestamptz - i * interval '1 second'
FROM generate_series(1, 3000) i;
ANALYZE known_routes;`)
	iatas := []string{"GPT", "YOW", "YVR", "YYJ"}
	for _, hop := range []int32{0, 3} {
		for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
			mustExec(t, ctx, tx, "SET LOCAL plan_cache_mode = "+mode)
			capture := &statementCapture{Tx: tx, name: "ListKnownRoutesByIATAs"}
			rows, err := (&Store{q: sqlc.New(unnamedDBTX{capture})}).listIATAsRouteRows(ctx, iatas, hop, time.Time{}, 0, 50)
			if err != nil || len(rows) != 50 {
				t.Fatalf("hop %d %s: %d rows (%v)", hop, mode, len(rows), err)
			}
			plan := explainJSON(t, ctx, tx, capture.statement, capture.args...)
			// A hop page reads the matching rows of each IATA's newest 200, a quarter of which match.
			budget := float64(len(iatas) * 50)
			if hop != 0 {
				budget *= 4
			}
			if visited := routeRowsVisited(t, plan); visited > budget+float64(len(iatas)) {
				t.Errorf("hop %d %s: examined %.0f routes, budget %.0f:\n%s", hop, mode, visited, budget, plan)
			}
		}
	}
}

// routeRowsVisited sums the known_routes rows an EXPLAIN ANALYZE plan returned or filtered out, over all loops.
func routeRowsVisited(t *testing.T, plan string) float64 {
	t.Helper()
	var plans []struct{ Plan map[string]any }
	if err := json.Unmarshal([]byte(plan), &plans); err != nil || len(plans) == 0 {
		t.Fatalf("EXPLAIN JSON did not parse (%v): %s", err, plan)
	}
	var visited float64
	var walk func(map[string]any)
	walk = func(p map[string]any) {
		if p["Relation Name"] == "known_routes" {
			rows, _ := p["Actual Rows"].(float64)
			removed, _ := p["Rows Removed by Filter"].(float64)
			loops, _ := p["Actual Loops"].(float64)
			visited += (rows + removed) * loops
		}
		if children, ok := p["Plans"].([]any); ok {
			for _, child := range children {
				walk(child.(map[string]any))
			}
		}
	}
	walk(plans[0].Plan)
	return visited
}
