// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
)

// legacyListKnownRoutes is origin/dev's ListKnownRoutes, verbatim. Args: iata, hopCount, cursor
// (pgtype.Timestamptz), limit (int32), cursorID (int64).
const legacyListKnownRoutes = `
SELECT r.id, r.node_ids, r.hash_prefix, r.iata, r.hop_count, r.first_seen, r.last_seen, r.observation_count
FROM ((
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE $1 = ''
  AND ($2 = 0 OR hop_count = $2)
  AND ($3::timestamptz IS NULL
       OR date_trunc('milliseconds', last_seen, 'UTC') < $3
       OR ($5::bigint > 0 AND date_trunc('milliseconds', last_seen, 'UTC') = $3 AND id < $5))
ORDER BY date_trunc('milliseconds', last_seen, 'UTC') DESC, id DESC
LIMIT $4
)
UNION ALL
(
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE $1 <> '' AND iata::text = $1
  AND iata >= $1::bpchar AND iata <= $1::bpchar
  AND ($2 = 0 OR hop_count = $2)
  AND ($3::timestamptz IS NULL
       OR date_trunc('milliseconds', last_seen, 'UTC') < $3
       OR ($5::bigint > 0 AND date_trunc('milliseconds', last_seen, 'UTC') = $3 AND id < $5))
ORDER BY iata, date_trunc('milliseconds', last_seen, 'UTC') DESC, id DESC
LIMIT $4
)) r
ORDER BY date_trunc('milliseconds', r.last_seen, 'UTC') DESC, r.id DESC
LIMIT $4
`

// routeFixture: 3 regions plus GPT (sparse), hops 2-5, one ms holding 5 tied routes (ids 11-15, µs apart),
// ids 16-17 one ms later, and 400 older routes one second apart. ANALYZE runs before GPT is added.
const routeFixture = `
INSERT INTO iata_codes (iata) VALUES ('GPT') ON CONFLICT DO NOTHING;
INSERT INTO known_routes (id,path_key,node_ids,hash_prefix,iata,hop_count,first_seen,last_seen,observation_count)
SELECT i, int8send(i), ARRAY[md5(i::text)::uuid], ARRAY['\x01'::bytea], (ARRAY['YYZ','YOW','YVR'])[1 + i % 3], 2 + i % 4,
       '2026-01-01', CASE WHEN i BETWEEN 11 AND 15 THEN '2026-01-10 00:00:00.123'::timestamptz + (i - 11) * interval '100 microseconds'
                          WHEN i IN (16, 17) THEN '2026-01-10 00:00:00.124'::timestamptz
                          ELSE '2026-01-10'::timestamptz - i * interval '1 second' END, 1 + i % 4
FROM generate_series(1, 400) i;
ANALYZE known_routes;
INSERT INTO known_routes (id,path_key,node_ids,hash_prefix,iata,hop_count,first_seen,last_seen)
SELECT 1000 + i, int8send(1000 + i), ARRAY[md5('g' || i)::uuid], ARRAY['\x02'::bytea], 'GPT', 2 + i % 4, '2026-01-01',
       '2026-01-09'::timestamptz - i * interval '1 second' FROM generate_series(1, 60) i;`

// routeStore is a Store over tx whose class C statements go unnamed, as New wires it.
func routeStore(tx pgx.Tx) *Store { return &Store{q: sqlc.New(tx), qc: sqlc.New(unnamedDBTX{tx})} }

func mustExec(t *testing.T, ctx context.Context, tx pgx.Tx, sql string) {
	t.Helper()
	if _, err := tx.Exec(ctx, sql); err != nil {
		t.Fatal(err)
	}
}

func ids(rows []knownRouteRow) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

// explainJSON runs EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) of a captured statement with its args.
func explainJSON(t *testing.T, ctx context.Context, tx pgx.Tx, sql string, args ...any) string {
	t.Helper()
	if sql == "" {
		t.Fatal("no statement captured")
	}
	rows, err := tx.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+sql, args...)
	if err != nil {
		t.Fatal(err)
	}
	plans, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(plans) != 1 {
		t.Fatalf("explain: %v (%d rows)", err, len(plans))
	}
	return plans[0]
}

// sharedBuffers is the top plan node's shared hit + read blocks.
func sharedBuffers(plan string) int {
	var plans []struct {
		Plan struct {
			Hit  int `json:"Shared Hit Blocks"`
			Read int `json:"Shared Read Blocks"`
		}
	}
	if err := json.Unmarshal([]byte(plan), &plans); err != nil || len(plans) == 0 {
		return -1
	}
	return plans[0].Plan.Hit + plans[0].Plan.Read
}
