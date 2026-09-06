// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	_ "embed"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

//go:embed queries/queries.sql
var routeTestQueries string

// All synthetic rows and indexes are temporary and rolled back. The sparse
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
	_, err = tx.Exec(ctx, `
CREATE TEMP TABLE known_routes (LIKE public.known_routes INCLUDING ALL) ON COMMIT DROP;
INSERT INTO known_routes (id,path_key,node_ids,hash_prefix,iata,hop_count,first_seen,last_seen)
SELECT i, decode(md5(i::text),'hex'), ARRAY[md5(i::text)::uuid], ARRAY['\x01'::bytea],
       CASE WHEN i > 100000 THEN 'GPT' ELSE 'YYZ' END, i % 4 + 2,
       '2026-01-01'::timestamptz - i * interval '1 second',
       '2026-01-01'::timestamptz - i * interval '1 second'
FROM generate_series(1,100100) i;
ANALYZE known_routes;`)
	if err != nil {
		t.Fatal(err)
	}
	migration, err := migrationFiles.ReadFile("migrations/025_known_routes_iata_last_seen.sql")
	if err != nil {
		t.Fatal(err)
	}
	// A temporary table inside this rollback-only fixture cannot be indexed
	// concurrently. Use the migration's exact definition with that option off.
	if _, err := tx.Exec(ctx, strings.Replace(string(migration), "CREATE INDEX CONCURRENTLY", "CREATE INDEX", 1)); err != nil {
		t.Fatal(err)
	}
	queries := strings.ReplaceAll(routeTestQueries, "\r\n", "\n")
	querySQL := strings.Split(strings.Split(queries, "-- name: ListKnownRoutes :many\n")[1], "\n-- name:")[0]
	if _, err := tx.Exec(ctx, "PREPARE route_page AS "+querySQL); err != nil {
		t.Fatal(err)
	}
	q := sqlc.New(tx)
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
					cursor := pgtype.Timestamptz{}
					if tc.cursor != 0 {
						cursor = pgtype.Timestamptz{Time: anchor.Add(-time.Duration(tc.cursor) * time.Second), Valid: true}
					}
					rows, err := q.ListKnownRoutes(ctx, sqlc.ListKnownRoutesParams{Column1: tc.iata, Column2: int32(tc.hop), Column3: cursor, Limit: int32(tc.limit)})
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
				var planJSON []byte
				if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE route_page('"+iata+"',0,NULL,50)").Scan(&planJSON); err != nil {
					t.Fatal(err)
				}
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
