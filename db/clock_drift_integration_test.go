// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
)

//go:embed queries/queries.sql
var clockTestQueries string

// Fixture writes are confined to temporary tables in a rolled-back transaction.
func TestClockDriftPagePostgres(t *testing.T) {
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
	for _, table := range []string{"nodes", "node_iatas"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	_, err = tx.Exec(ctx, `
INSERT INTO nodes (id,public_key,node_type,name,device_clock_drift_seconds,last_advert_at)
SELECT ('00000000-0000-0000-0000-'||lpad(i::text,12,'0'))::uuid,decode(lpad(to_hex(i),64,'0'),'hex'),kind,
 CASE WHEN i=2 THEN NULL ELSE 'node-'||i END,drift,CASE WHEN i=2 THEN NULL ELSE '2026-01-01'::timestamptz END
FROM (VALUES (1,2,120),(2,3,-240),(3,1,99999),(4,4,-99999),(5,2,NULL),(6,2,60),(7,3,-60),(8,2,180)) v(i,kind,drift);
INSERT INTO node_iatas (node_id,iata,last_heard) VALUES
 ('00000000-0000-0000-0000-000000000001','YVR','2026-01-01'),
 ('00000000-0000-0000-0000-000000000001','YYJ','2026-01-01 01:00:00+00'),
 ('00000000-0000-0000-0000-000000000002','YYJ','2026-01-01');`)
	if err != nil {
		t.Fatal(err)
	}
	q := sqlc.New(tx)
	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name             string
		iatas            []string
		threshold, limit int32
		want             []int
	}{
		{"global", nil, 60, 10, []int{2, 8, 1}},
		{"empty region array", []string{}, 60, 10, []int{2, 8, 1}},
		{"page limit", nil, 60, 2, []int{2, 8}},
		{"YVR", []string{"YVR"}, 60, 10, []int{1}},
		{"YYJ", []string{"YYJ"}, 60, 10, []int{2, 1}},
		{"multiple regions", []string{"YVR", "YYJ"}, 60, 10, []int{2, 1}},
		{"duplicate region", []string{"YYJ", "YYJ"}, 60, 10, []int{2, 1}},
		{"unknown region", []string{"ZZZ"}, 60, 10, nil},
		{"strict threshold", nil, 180, 10, []int{2}},
		{"boundary", nil, 240, 10, nil},
		{"positive and negative boundary", nil, 0, 10, []int{2, 8, 1, 6, 7}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := q.GetStatsClockDrift(ctx, sqlc.GetStatsClockDriftParams{Column1: tc.threshold, Column2: tc.iatas, Limit: tc.limit})
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != len(tc.want) {
				t.Fatalf("got %d rows, want %d", len(rows), len(tc.want))
			}
			previous := int64(1 << 62)
			seen := map[int]bool{}
			for _, row := range rows {
				var number int
				if _, err := fmt.Sscanf(row.ID.String(), "00000000-0000-0000-0000-%d", &number); err != nil {
					t.Fatal(err)
				}
				if !slices.Contains(tc.want, number) || seen[number] {
					t.Fatalf("unexpected/duplicate node %d", number)
				}
				seen[number] = true
				if row.DeviceClockDriftSeconds == nil {
					t.Fatal("null drift returned")
				}
				wantDrift := map[int]int32{1: 120, 2: -240, 6: 60, 7: -60, 8: 180}[number]
				if *row.DeviceClockDriftSeconds != wantDrift || (row.NodeType != 2 && row.NodeType != 3) {
					t.Fatal("drift sign or role changed")
				}
				magnitude := int64(wantDrift)
				if magnitude < 0 {
					magnitude = -magnitude
				}
				if magnitude > previous {
					t.Fatal("not ordered worst first")
				}
				previous = magnitude
				if number == 2 {
					if row.Name != nil || row.LastAdvertAt.Valid {
						t.Fatal("nullable node fields changed")
					}
				} else if row.Name == nil || *row.Name != fmt.Sprintf("node-%d", number) || !row.LastAdvertAt.Valid || !row.LastAdvertAt.Time.Equal(anchor) {
					t.Fatal("node fields changed")
				}
				var got []api.NodeIATA
				if len(row.Iatas) > 0 {
					if err := json.Unmarshal(row.Iatas, &got); err != nil {
						t.Fatal(err)
					}
				}
				var want []api.NodeIATA
				if number == 1 {
					want = []api.NodeIATA{{IATA: "YYJ", LastHeard: anchor.Add(time.Hour).UnixMilli()}, {IATA: "YVR", LastHeard: anchor.UnixMilli()}}
				}
				if number == 2 {
					want = []api.NodeIATA{{IATA: "YYJ", LastHeard: anchor.UnixMilli()}}
				}
				if !slices.Equal(got, want) {
					t.Fatalf("node %d memberships/order: got %v, want %v", number, got, want)
				}
			}
		})
	}
	// Use a separate large population for the work-budget measurement.
	_, err = tx.Exec(ctx, `
TRUNCATE pg_temp.node_iatas,pg_temp.nodes;
INSERT INTO nodes (id,public_key,node_type,name,device_clock_drift_seconds,last_advert_at)
SELECT md5(i::text)::uuid,decode(lpad(to_hex(i),64,'0'),'hex'),2,'clock-'||i,3600+i,'2026-01-01'
FROM generate_series(1,20000) i;
INSERT INTO node_iatas (node_id,iata,last_heard)
SELECT md5(i::text)::uuid,iata,'2026-01-01'::timestamptz-i*interval '1 second'
FROM generate_series(1,20000) i CROSS JOIN (VALUES ('YVR'),('YYJ')) regions(iata);
ANALYZE nodes; ANALYZE node_iatas;`)
	if err != nil {
		t.Fatal(err)
	}
	queries := strings.ReplaceAll(clockTestQueries, "\r\n", "\n")
	query := strings.Split(strings.Split(queries, "-- name: GetStatsClockDrift :many\n")[1], "\n-- name:")[0]
	if _, err := tx.Exec(ctx, "PREPARE clock_page AS "+query); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		if _, err := tx.Exec(ctx, "SET LOCAL plan_cache_mode = "+mode); err != nil {
			t.Fatal(err)
		}
		var raw []byte
		if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE clock_page(60,NULL,50)").Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var plans []struct {
			Plan          map[string]any
			ExecutionTime float64 `json:"Execution Time"`
		}
		if err := json.Unmarshal(raw, &plans); err != nil {
			t.Fatal(err)
		}
		var memberships float64
		var walk func(map[string]any)
		walk = func(p map[string]any) {
			if p["Relation Name"] == "node_iatas" {
				memberships += p["Actual Rows"].(float64) * p["Actual Loops"].(float64)
			}
			if children, ok := p["Plans"].([]any); ok {
				for _, child := range children {
					walk(child.(map[string]any))
				}
			}
		}
		walk(plans[0].Plan)
		t.Logf("20,000 nodes / 50-row clock page %s: %.3f ms, %.0f membership rows processed", mode, plans[0].ExecutionTime, memberships)
		if memberships > 150 {
			t.Errorf("clock page processed %.0f membership rows; budget 150", memberships)
		}
	}
}
