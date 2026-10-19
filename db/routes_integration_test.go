// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
)

// All synthetic rows are rolled back. The sparse
// region sits behind 100,000 newer routes to expose global timestamp scans.
func TestListKnownRoutesPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for the PostgreSQL regression test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	applyBaseline(t, ctx, tx)
	_, err = tx.Exec(ctx, `
INSERT INTO iata_codes (iata) VALUES ('GPT');
INSERT INTO known_routes (id,path_key,node_ids,hash_prefix,iata,hop_count,first_seen,last_seen)
SELECT i, decode(md5(i::text),'hex'), ARRAY[md5(i::text)::uuid], ARRAY['\x01'::bytea],
       CASE WHEN i > 100000 THEN 'GPT' ELSE 'YYZ' END, i % 4 + 2,
       '2026-01-01 00:00:00+00'::timestamptz - i * interval '1 second',
       '2026-01-01 00:00:00+00'::timestamptz - i * interval '1 second'
FROM generate_series(1,100100) i;
ANALYZE known_routes;`)
	if err != nil {
		t.Fatal(err)
	}
	s := routeStore(tx)
	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		t.Run(mode, func(t *testing.T) {
			if _, err := tx.Exec(ctx, "SET LOCAL plan_cache_mode = "+mode); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name, iata                               string
				hop, cursor, limit, first, stride, count int
			}{
				{"global", "", 0, 0, 3, 1, 1, 3},
				{"common", "YYZ", 0, 0, 3, 1, 1, 3},
				{"sparse", "GPT", 0, 0, 50, 100001, 1, 50},
				{"sparse hop", "GPT", 2, 0, 50, 100004, 4, 25},
				{"global hop", "", 2, 0, 3, 4, 4, 3},
				{"global cursor", "", 0, 3, 3, 4, 1, 3},
				{"sparse cursor", "GPT", 0, 100010, 3, 100011, 1, 3},
				{"sparse hop cursor", "GPT", 2, 100010, 3, 100012, 4, 3},
				{"partial page", "GPT", 0, 100098, 50, 100099, 1, 2},
				{"end", "GPT", 0, 100100, 50, 0, 1, 0},
				{"unknown", "ZZZ", 0, 0, 50, 0, 1, 0},
				{"lowercase", "gpt", 0, 0, 50, 0, 1, 0},
				{"trailing space", "GPT ", 0, 0, 50, 0, 1, 0},
				{"overlong", "GPTX", 0, 0, 50, 0, 1, 0},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var cursor time.Time
					if tc.cursor != 0 {
						cursor = anchor.Add(-time.Duration(tc.cursor) * time.Second)
					}
					rows, err := s.listKnownRouteRows(ctx, tc.iata, int32(tc.hop), cursor, 0, int32(tc.limit))
					if err != nil {
						t.Fatal(err)
					}
					if len(rows) != tc.count {
						t.Fatalf("got %d rows, want %d", len(rows), tc.count)
					}
					for i, r := range rows {
						want := int64(tc.first + i*tc.stride)
						wantIATA := "YYZ"
						if want > 100000 {
							wantIATA = "GPT"
						}
						if r.ID != want || r.Iata != wantIATA || r.HopCount != int32(want%4+2) || r.ObservationCount != 1 ||
							len(r.NodeIds) != 1 || len(r.HashPrefix) != 1 || len(r.HashPrefix[0]) != 1 || r.HashPrefix[0][0] != 1 ||
							!r.LastSeen.Time.Equal(anchor.Add(-time.Duration(want)*time.Second)) || !r.FirstSeen.Time.Equal(r.LastSeen.Time) {
							t.Errorf("unexpected route %d at row %d; want %d", r.ID, i, want)
						}
					}
				})
			}
			for _, iata := range []string{"GPT", "YYZ", ""} {
				// A fresh capture per page: names match by prefix, so ...Global would also take ...GlobalHop.
				capture := &statementCapture{Tx: tx, name: "ListKnownRoutesByIATA"}
				if iata == "" {
					capture.name = "ListKnownRoutesGlobal"
				}
				if _, err := (&Store{q: sqlc.New(capture), qc: sqlc.New(unnamedDBTX{capture})}).listKnownRouteRows(ctx, iata, 0, time.Time{}, 0, 50); err != nil {
					t.Fatal(err)
				}
				planJSON := []byte(explainJSON(t, ctx, tx, capture.statement, capture.args...))
				var plans []struct {
					Plan          map[string]any
					ExecutionTime float64 `json:"Execution Time"`
				}
				if err := json.Unmarshal(planJSON, &plans); err != nil {
					t.Fatal(err)
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
				t.Logf("100,100 routes / 50-row page IATA=%q: %.3f ms, %.0f rows examined", iata, plans[0].ExecutionTime, visited)
				if visited > 200 {
					t.Errorf("page IATA=%q examined %.0f routes; budget 200", iata, visited)
				}
			}
		})
	}
}
