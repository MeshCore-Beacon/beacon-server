// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

//go:embed queries/queries.sql
var nodeTestQueries string

// Run against a migrated PostgreSQL database with BEACON_TEST_POSTGRES_DSN.
// All fixture writes are transaction-local temporary tables, rolled back on exit.
func TestListNodesPostgres(t *testing.T) {
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
	for _, table := range []string{"nodes", "node_iatas", "node_neighbors", "observers", "transport_scopes"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	_, err = tx.Exec(ctx, `
INSERT INTO transport_scopes (id, name, transport_key, key_fingerprint) VALUES (1, '#test', decode(repeat('00', 16), 'hex'), decode(repeat('00', 8), 'hex'));
INSERT INTO nodes (id, public_key, node_type, name, last_seen, default_scope_id, supports_multibyte_paths, supports_multibyte_traces)
SELECT md5(i::text)::uuid, decode(lpad(to_hex(i), 64, '0'), 'hex'), (i % 4 + 1)::smallint,
       'node-' || i, '2026-01-01'::timestamptz - i * interval '1 second',
       CASE WHEN i % 2 = 0 THEN 1 END, i % 2 = 0, i % 3 = 0
FROM generate_series(1, 20000) i;
INSERT INTO node_iatas (node_id, iata, last_heard)
SELECT md5(i::text)::uuid, 'YVR', '2026-01-01'::timestamptz - i * interval '1 second'
FROM generate_series(1, 19999) i;
INSERT INTO node_iatas (node_id, iata, last_heard)
SELECT md5(i::text)::uuid, 'YYJ', '2026-01-01'::timestamptz - i * interval '1 second' - interval '1 hour'
FROM generate_series(2, 19998, 2) i;
INSERT INTO node_neighbors (node_id, neighbor_id, iata)
SELECT md5(i::text)::uuid, md5((i+1)::text)::uuid, iata
FROM generate_series(1, 19999) i CROSS JOIN (VALUES ('YVR'), ('YYJ')) regions(iata);
INSERT INTO observers (id, public_key)
SELECT id, public_key FROM nodes WHERE name = 'node-10';
ANALYZE nodes; ANALYZE node_iatas; ANALYZE node_neighbors; ANALYZE observers; ANALYZE transport_scopes;
`)
	if err != nil {
		t.Fatal(err)
	}
	query := sqlc.New(tx)
	base := sqlc.ListNodesParams{Column1: int16(0), Column3: "any", Column4: "any", Column6: "", Limit: 51}
	for _, tc := range []struct {
		name   string
		change func(*sqlc.ListNodesParams)
		want   []int
	}{
		{"page", func(p *sqlc.ListNodesParams) { p.Limit = 3 }, []int{1, 2, 3}},
		{"type", func(p *sqlc.ListNodesParams) { p.Column1 = 2; p.Limit = 3 }, []int{1, 5, 9}},
		{"multi IATA dedup", func(p *sqlc.ListNodesParams) { p.Column2 = []string{"YVR", "YYJ"}; p.Limit = 3 }, []int{1, 2, 3}},
		{"IATA", func(p *sqlc.ListNodesParams) { p.Column2 = []string{"YYJ"}; p.Limit = 3 }, []int{2, 4, 6}},
		{"unknown IATA", func(p *sqlc.ListNodesParams) { p.Column2 = []string{"ZZZ"} }, nil},
		{"scope", func(p *sqlc.ListNodesParams) { p.Column9 = "#test"; p.Limit = 3 }, []int{2, 4, 6}},
		{"capabilities", func(p *sqlc.ListNodesParams) { p.Column3 = "true"; p.Column4 = "false"; p.Limit = 3 }, []int{2, 4, 8}},
		{"name", func(p *sqlc.ListNodesParams) { p.Column6 = "NODE-19"; p.Limit = 3 }, []int{19, 190, 191}},
		{"cursor", func(p *sqlc.ListNodesParams) {
			p.Column7 = pgtype.Timestamptz{Time: time.Date(2026, 1, 1, 0, 0, -3, 0, time.UTC), Valid: true}
			p.Limit = 3
		}, []int{4, 5, 6}},
		{"public key", func(p *sqlc.ListNodesParams) { p.Column5, _ = hex.DecodeString(fmt.Sprintf("%064x", 10)) }, []int{10}},
		{"public key prefix", func(p *sqlc.ListNodesParams) { p.Column11 = strings.Repeat("0", 61) + "00A" }, []int{10}},
		{"no IATA", func(p *sqlc.ListNodesParams) { p.Column5, _ = hex.DecodeString(fmt.Sprintf("%064x", 20000)) }, []int{20000}},
	} {
		for _, neighbors := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/neighbors=%t", tc.name, neighbors), func(t *testing.T) {
				params := base
				tc.change(&params)
				params.Column10 = neighbors
				rows, err := query.ListNodes(ctx, params)
				if err != nil {
					t.Fatal(err)
				}
				if len(rows) != len(tc.want) {
					t.Fatalf("got %d rows, want %d", len(rows), len(tc.want))
				}
				for i, number := range tc.want {
					row := rows[i]
					if row.Name == nil || *row.Name != fmt.Sprintf("node-%d", number) {
						t.Fatalf("wrong row at %d: %v", i, row.Name)
					}
					var iatas []struct {
						IATA string `json:"iata"`
					}
					if len(row.Iatas) > 0 {
						if err := json.Unmarshal(row.Iatas, &iatas); err != nil {
							t.Fatal(err)
						}
					}
					wantIATAs := 1
					if number%2 == 0 {
						wantIATAs = 2
					}
					wantNeighbors := int64(1)
					if number == 20000 {
						wantIATAs, wantNeighbors = 0, 0
					}
					if len(iatas) != wantIATAs || (len(iatas) > 0 && iatas[0].IATA != "YVR") {
						t.Errorf("wrong IATAs: %s", row.Iatas)
					}
					if row.KnownNeighborCount != wantNeighbors {
						t.Errorf("neighbor count %d, want %d", row.KnownNeighborCount, wantNeighbors)
					}
					if neighbors && len(row.NeighborIds) != int(wantNeighbors) {
						t.Errorf("neighbor IDs %v", row.NeighborIds)
					}
					if !neighbors && len(row.NeighborIds) != 0 {
						t.Errorf("unrequested neighbor IDs %v", row.NeighborIds)
					}
					if row.IsObserver != (number == 10) {
						t.Error("wrong observer flag")
					}
					if number == 10 && row.ObserverID != row.ID {
						t.Error("wrong observer ID")
					}
				}
			})
		}
	}

	// A small page must not aggregate every node's IATA membership. Assert work,
	// not elapsed time: shared CI and Pi hosts have variable scheduling latency.
	queries := strings.ReplaceAll(nodeTestQueries, "\r\n", "\n")
	sql := strings.Split(strings.Split(queries, "-- name: ListNodes :many\n")[1], "\n-- name:")[0]
	var planJSON []byte
	err = tx.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+sql,
		int16(0), []string(nil), "any", "any", []byte(nil), "", nil, int32(51), "", false, "").Scan(&planJSON)
	if err != nil {
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
	walk = func(plan map[string]any) {
		if plan["Relation Name"] == "node_iatas" {
			visited += plan["Actual Rows"].(float64) * plan["Actual Loops"].(float64)
		}
		if children, ok := plan["Plans"].([]any); ok {
			for _, child := range children {
				walk(child.(map[string]any))
			}
		}
	}
	walk(plans[0].Plan)
	t.Logf("20,000 nodes / 51-row page: execution %.3f ms; IATA rows processed %.0f", plans[0].ExecutionTime, visited)
	if visited > 200 {
		t.Errorf("small node page processed %.0f IATA rows; budget 200", visited)
	}
}
