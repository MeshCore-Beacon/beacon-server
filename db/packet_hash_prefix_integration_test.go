// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"os"
	"slices"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
)

func TestSearchPacketsByHashPrefixPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for the PostgreSQL regression test")
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
	for _, table := range []string{"packets", "packet_observations", "observers", "transport_scopes"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	// Newest first by last_heard_at: fb00 > fbff > fc00 > fa00 > ffff.
	_, err = tx.Exec(ctx, `
INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header, first_heard_at, last_heard_at) VALUES
  ('\x6ddf62fb00000000', 2, 0, 1, '\x00', '\x00', NOW(), NOW()),
  ('\x6ddf62fbffffffff', 2, 0, 1, '\x00', '\x00', NOW(), NOW() - interval '1 minute'),
  ('\x6ddf62fc00000000', 2, 0, 1, '\x00', '\x00', NOW(), NOW() - interval '2 minutes'),
  ('\x6ddf62fa00000000', 2, 0, 1, '\x00', '\x00', NOW(), NOW() - interval '3 minutes'),
  ('\xffffffffffffffff', 2, 0, 1, '\x00', '\x00', NOW(), NOW() - interval '4 minutes');
INSERT INTO observers (id, public_key) VALUES ('00000000-0000-0000-0000-000000000001', '\x02');
INSERT INTO packet_observations (id, packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count, path_bytes)
VALUES (1, '\x6ddf62fbffffffff', '00000000-0000-0000-0000-000000000001', 'YOW', NOW(), 0, 1, 0, '\x');`)
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	search := func(lo, hi string, iatas []string, cursor int64, limit int32) api.Page[api.PacketSummary] {
		t.Helper()
		loB, _ := hex.DecodeString(lo)
		var hiB []byte
		if hi != "" {
			hiB, _ = hex.DecodeString(hi)
		}
		page, err := store.SearchPacketsByHashPrefix(ctx, loB, hiB, nil, nil, iatas, nil, time.Time{}, time.Time{}, cursor, limit)
		if err != nil {
			t.Fatal(err)
		}
		return page
	}
	hashes := func(p api.Page[api.PacketSummary]) []string {
		var out []string
		for _, it := range p.Items {
			out = append(out, it.PacketHash)
		}
		return out
	}

	if got := hashes(search("6ddf62fb", "6ddf62fc", nil, 0, 50)); !slices.Equal(got, []string{"6ddf62fb00000000", "6ddf62fbffffffff"}) {
		t.Errorf("8-char prefix: %v", got)
	}
	if got := hashes(search("6ddf60", "6ddf70", nil, 0, 50)); len(got) != 4 {
		t.Errorf("odd prefix: %v", got)
	}
	if got := hashes(search("ffff", "", nil, 0, 50)); !slices.Equal(got, []string{"ffffffffffffffff"}) {
		t.Errorf("unbounded prefix: %v", got)
	}
	if got := hashes(search("6ddf62fb", "6ddf62fc", []string{"YOW"}, 0, 50)); !slices.Equal(got, []string{"6ddf62fbffffffff"}) {
		t.Errorf("iata filter: %v", got)
	}
	first := search("6ddf62fb", "6ddf62fc", nil, 0, 1)
	if !first.HasMore || first.NextCursor == nil || hashes(first)[0] != "6ddf62fb00000000" {
		t.Fatalf("first page: %+v", first)
	}
	if got := hashes(search("6ddf62fb", "6ddf62fc", nil, *first.NextCursor, 1)); !slices.Equal(got, []string{"6ddf62fbffffffff"}) {
		t.Errorf("second page: %v", got)
	}
}
