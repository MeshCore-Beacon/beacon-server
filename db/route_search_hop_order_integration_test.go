// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"strings"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
)

// S3a adds (hop_count, iata); a hop-ordered LIMIT walk over it reads most of known_routes when matches are rare.
func TestRouteSearchHopOrderPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	if _, err := tx.Exec(ctx, `
INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count, last_seen)
SELECT int4send(i), ARRAY[md5(i::text)::uuid, md5('a'||(i%50))::uuid, md5('b'||(i%7))::uuid], ARRAY['\x01'::bytea],
       CASE WHEN i%2=0 THEN 'YVR' ELSE 'YYJ' END, 2 + i%30, now() - make_interval(secs => i)
FROM generate_series(1,20000) i;
CREATE INDEX idx_known_routes_hop_iata ON known_routes (hop_count, iata);
ANALYZE known_routes;
SET LOCAL enable_sort = off`); err != nil {
		t.Fatal(err)
	}
	var from, to uuid.UUID
	if err := tx.QueryRow(ctx, "SELECT md5('a3')::uuid, md5('b3')::uuid").Scan(&from, &to); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		run  func(q *sqlc.Queries) error
	}{
		{"SearchKnownRoutesByNodes", func(q *sqlc.Queries) error {
			_, err := q.SearchKnownRoutesByNodes(ctx, sqlc.SearchKnownRoutesByNodesParams{FromNodes: []uuid.UUID{from}, ToNodes: []uuid.UUID{to}, Iatas: []string{"YVR", "YYJ"}, RowLimit: routeSearchLimit})
			return err
		}},
		{"GetKnownRoutesByNodes", func(q *sqlc.Queries) error {
			_, err := q.GetKnownRoutesByNodes(ctx, sqlc.GetKnownRoutesByNodesParams{Nodes: []uuid.UUID{from, to}, Iatas: []string{"YVR", "YYJ"}, RowLimit: crossRouteRowLimit})
			return err
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			capture := &statementCapture{Tx: tx, name: c.name}
			if err := c.run(sqlc.New(capture)); err != nil {
				t.Fatal(err)
			}
			var plan string
			if err := tx.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+capture.statement, capture.args...).Scan(&plan); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(plan, "idx_known_routes_hop_iata") {
				t.Fatalf("a hop_count index supplies the route order: %s", plan)
			}
		})
	}
}
