// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
)

func retentionTx(t *testing.T) (context.Context, pgx.Tx) {
	t.Helper()
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for the PostgreSQL regression test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback(context.Background()) })
	return ctx, tx
}

func countRows(t *testing.T, ctx context.Context, tx pgx.Tx, sql string) int {
	t.Helper()
	var n int
	if err := tx.QueryRow(ctx, sql).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// More than one batch of old packets; observations go with them.
func TestDeleteOldPacketsBatchesPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	_, err := tx.Exec(ctx, `
CREATE TEMP TABLE packets (LIKE public.packets INCLUDING ALL) ON COMMIT DROP;
CREATE TEMP TABLE packet_observations (LIKE public.packet_observations INCLUDING ALL) ON COMMIT DROP;
ALTER TABLE packet_observations ADD FOREIGN KEY (packet_hash) REFERENCES packets(packet_hash) ON DELETE CASCADE;
INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header, first_heard_at, last_heard_at)
SELECT int4send(i), 4, 0, 1, '\x00', '\x00',
       CASE WHEN i <= 2500 THEN '2026-01-01'::timestamptz ELSE '2026-02-01'::timestamptz END,
       CASE WHEN i <= 2500 THEN '2026-01-01'::timestamptz ELSE '2026-02-01'::timestamptz END
FROM generate_series(1, 2510) i;
INSERT INTO packet_observations (packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count)
SELECT packet_hash, '00000000-0000-0000-0000-000000000001', 'YVR', last_heard_at, 0, 1, 0 FROM packets;`)
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	if err := store.DeleteOldPackets(ctx, time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, ctx, tx, `SELECT count(*) FROM packets`); n != 10 {
		t.Errorf("packets left = %d, want 10", n)
	}
	if n := countRows(t, ctx, tx, `SELECT count(*) FROM packet_observations`); n != 10 {
		t.Errorf("observations left = %d, want 10", n)
	}
}

// Covers grace set longer than retention too.
func TestDeleteOldRoutesBatchesPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	// 1-10500 past retention, 10501-10600 rare, 10601-10700 well observed, rest fresh.
	_, err := tx.Exec(ctx, `
CREATE TEMP TABLE known_routes (LIKE public.known_routes INCLUDING ALL) ON COMMIT DROP;
INSERT INTO known_routes (id, path_key, node_ids, hash_prefix, iata, hop_count, last_seen, observation_count)
SELECT i, decode(md5(i::text), 'hex'), ARRAY[md5(i::text)::uuid], ARRAY['\x01'::bytea], 'YYZ', 2,
       CASE WHEN i <= 10500 THEN '2026-01-01'::timestamptz
            WHEN i <= 10700 THEN '2026-01-20'::timestamptz
            ELSE '2026-02-01'::timestamptz END,
       CASE WHEN i BETWEEN 10501 AND 10600 THEN 1 ELSE 10 END
FROM generate_series(1, 10710) i;`)
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	retention := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	grace := time.Date(2026, 1, 25, 0, 0, 0, 0, time.UTC)
	if err := store.DeleteOldRoutes(ctx, retention, 3, grace); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, ctx, tx, `SELECT count(*) FROM known_routes`); n != 110 {
		t.Errorf("routes left = %d, want 110 (well-observed + fresh)", n)
	}
	if n := countRows(t, ctx, tx, `SELECT count(*) FROM known_routes WHERE observation_count = 1`); n != 0 {
		t.Errorf("rare routes past grace left = %d, want 0", n)
	}

	// Grace longer than retention: retention alone decides.
	if err := store.DeleteOldRoutes(ctx, time.Date(2026, 1, 25, 0, 0, 0, 0, time.UTC), 3, retention); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, ctx, tx, `SELECT count(*) FROM known_routes`); n != 10 {
		t.Errorf("routes left = %d, want 10 fresh", n)
	}
}
