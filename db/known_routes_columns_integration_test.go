// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"slices"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

// knownRoutesReAdd is the rollback statement for the column drop: older images read or stamp the column.
const knownRoutesReAdd = "ALTER TABLE known_routes ADD COLUMN last_reconfirmed_at timestamp with time zone DEFAULT now() NOT NULL"

// known_routes keeps exactly the columns a statement reads, and putting the dropped one back is a catalog-only change.
func TestKnownRoutesColumnsPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var cols []string
	if err := pool.QueryRow(ctx, `SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute
WHERE attrelid = 'known_routes'::regclass AND attnum > 0 AND NOT attisdropped`).Scan(&cols); err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "path_key", "node_ids", "hash_prefix", "iata", "hop_count", "first_seen", "last_seen", "observation_count"}
	if !slices.Equal(cols, want) {
		t.Fatalf("known_routes columns = %v, want %v", cols, want)
	}

	if _, err := pool.Exec(ctx, `INSERT INTO iata_codes (iata) VALUES ('YVR') ON CONFLICT DO NOTHING;
INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count, observation_count)
VALUES ('\x0102', ARRAY[gen_random_uuid(), gen_random_uuid()], ARRAY['\x01'::bytea, '\x02'::bytea], 'YVR', 2, 3)`); err != nil {
		t.Fatal(err)
	}
	route, err := sqlc.New(pool).GetRouteEvidenceRoute(ctx, sqlc.GetRouteEvidenceRouteParams{Iata: "YVR", PathKey: []byte{1, 2}})
	if err != nil || route.HopCount != 2 || route.ObservationCount != 3 || len(route.NodeIds) != 2 {
		t.Fatalf("GetRouteEvidenceRoute = %+v, %v", route, err)
	}

	// The rollback re-add keeps the table's file (no rewrite) and gives existing rows a value.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // the re-add is only tried
	var before, after uint32
	if err := tx.QueryRow(ctx, "SELECT pg_relation_filenode('known_routes')").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, knownRoutesReAdd); err != nil {
		t.Fatal(err)
	}
	var nulls int
	if err := tx.QueryRow(ctx, "SELECT pg_relation_filenode('known_routes'), count(*) FILTER (WHERE last_reconfirmed_at IS NULL) FROM known_routes").Scan(&after, &nulls); err != nil {
		t.Fatal(err)
	}
	if after != before || nulls != 0 {
		t.Errorf("re-add: filenode %d -> %d (want unchanged), %d NULL stamps", before, after, nulls)
	}
}
