// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/jackc/pgx/v5"
)

func TestChannelMessageCountPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
	store := &Store{q: sqlc.New(tx)}

	if _, err := tx.Exec(ctx, `INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header, first_heard_at, last_heard_at)
SELECT decode(lpad(to_hex(i), 2, '0'), 'hex'), 5, 0, 1, '\x00', '\x00', now(), now() FROM generate_series(1, 2) i`); err != nil {
		t.Fatal(err)
	}
	// Re-upserting the channel (as every decrypt does) must not touch the count.
	var id int
	for range 3 {
		if id, err = store.UpsertChannel(ctx, []byte{0x11}, []byte{1, 2, 3, 4, 5, 6, 7, 8}, "public", ""); err != nil {
			t.Fatal(err)
		}
	}
	for _, hash := range [][]byte{{1}, {1}, {2}} { // second {1} is a duplicate
		if _, err := store.InsertChannelMessage(ctx, ingest.InsertChannelMessageParams{
			ChannelID: id, PacketHash: hash, SenderName: "a", Content: "b", SentAt: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	ch, err := store.GetChannel(ctx, int32(id))
	if err != nil {
		t.Fatal(err)
	}
	if ch.MessageCount != 2 {
		t.Errorf("message_count = %d, want 2", ch.MessageCount)
	}
}
