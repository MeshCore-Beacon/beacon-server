// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/jackc/pgx/v5"
)

func TestPacketObservationCountPostgres(t *testing.T) {
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

	hash := []byte{0xab, 0xcd}
	heard := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := tx.Exec(ctx, `INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header, first_heard_at, last_heard_at)
VALUES ($1, 4, 0, 1, '\x00', '\x00', $2, $2)`, hash, heard); err != nil {
		t.Fatal(err)
	}
	a, _, err := store.UpsertObserver(ctx, bytes.Repeat([]byte{1}, 32), "YVR")
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := store.UpsertObserver(ctx, bytes.Repeat([]byte{2}, 32), "YYZ")
	if err != nil {
		t.Fatal(err)
	}

	steps := []struct {
		obs          ingest.InsertObservationParams
		wantInserted bool
		wantCount    int64
	}{
		{ingest.InsertObservationParams{PacketHash: hash, ObserverID: a, IATA: "YVR", HeardAt: heard}, true, 1},
		{ingest.InsertObservationParams{PacketHash: hash, ObserverID: b, IATA: "YYZ", HeardAt: heard}, true, 2},
		{ingest.InsertObservationParams{PacketHash: hash, ObserverID: a, IATA: "YVR", HeardAt: heard}, false, 2}, // broker duplicate
	}
	for i, s := range steps {
		inserted, count, err := store.InsertObservation(ctx, s.obs)
		if err != nil {
			t.Fatal(err)
		}
		if inserted != s.wantInserted || count != s.wantCount {
			t.Errorf("step %d: inserted=%v count=%d, want %v, %d", i, inserted, count, s.wantInserted, s.wantCount)
		}
	}

	var stored, raw int64
	if err := tx.QueryRow(ctx, `SELECT p.observation_count, (SELECT count(*) FROM packet_observations po WHERE po.packet_hash = p.packet_hash)
FROM packets p WHERE p.packet_hash = $1`, hash).Scan(&stored, &raw); err != nil {
		t.Fatal(err)
	}
	if stored != raw || stored != 2 {
		t.Errorf("packets.observation_count = %d, raw count = %d; want 2", stored, raw)
	}

	page, err := store.ListPackets(ctx, nil, nil, nil, nil, time.Time{}, time.Time{}, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ObservationCount != 2 {
		t.Errorf("ListPackets observation counts = %+v, want one packet with 2", page.Items)
	}
}
