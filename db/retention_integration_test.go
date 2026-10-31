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
	applyBaseline(t, ctx, tx)
	_, err := tx.Exec(ctx, `
INSERT INTO observers (id, public_key) VALUES ('00000000-0000-0000-0000-000000000001', '\x01');
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
