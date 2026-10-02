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
	"github.com/MeshCore-Beacon/beacon-server/internal/presence"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestObserverLastIATAPostgres(t *testing.T) {
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
	c := presence.New(store, time.Hour, time.Hour)
	pkA, pkB, pkC := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32), bytes.Repeat([]byte{3}, 32)
	ids := map[string]uuid.UUID{}
	heard := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Mirrors ingest: upsert the observer, then record the observation.
	packet := func(name string, pk []byte, iata string) {
		t.Helper()
		id, _, err := c.UpsertObserver(ctx, pk, iata)
		if err != nil {
			t.Fatal(err)
		}
		ids[name] = id
		heard = heard.Add(time.Second)
		hash := []byte(heard.String())
		if _, err := tx.Exec(ctx, `INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header, first_heard_at, last_heard_at)
VALUES ($1, 4, 0, 1, '\x00', '\x00', $2, $2)`, hash, heard); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO packet_observations (packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count)
VALUES ($1, $2, $3, $4, 0, 1, 0)`, hash, id, iata, heard); err != nil {
			t.Fatal(err)
		}
	}
	status := func(pk []byte) {
		t.Helper()
		if _, _, err := c.UpsertObserver(ctx, pk, ""); err != nil {
			t.Fatal(err)
		}
	}

	packet("A", pkA, "YVR")
	packet("A", pkA, "YYZ")
	status(pkA)
	packet("B", pkB, "YOW")
	if _, err := c.UpdateObserverStatus(ctx, ingest.UpdateObserverStatusParams{PublicKey: pkB, DisplayName: "b"}); err != nil {
		t.Fatal(err)
	}
	packet("B", pkB, "YYJ") // write-through after the identity was dropped
	status(pkC)
	c.Flush(ctx)
	packet("A", pkA, "YVR") // pending in the coalescer, not yet flushed
	c.Flush(ctx)

	rows, err := tx.Query(ctx, `SELECT o.id, COALESCE(o.last_iata, ''), COALESCE((
    SELECT po.iata FROM packet_observations po WHERE po.observer_id = o.id ORDER BY po.heard_at DESC LIMIT 1), '')
FROM observers o`)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		var id uuid.UUID
		var got, want string
		if err := rows.Scan(&id, &got, &want); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("observer %s last_iata = %q, latest observation = %q", id, got, want)
		}
		n++
	}
	if rows.Err() != nil || n != 3 {
		t.Fatalf("observers = %d, err %v; want 3", n, rows.Err())
	}

	for name, want := range map[string]string{"A": "YVR", "B": "YYJ"} {
		if got, err := store.GetObserverLastIATA(ctx, ids[name]); err != nil || got != want {
			t.Errorf("GetObserverLastIATA(%s) = %q, %v; want %q", name, got, err, want)
		}
	}
	var idC uuid.UUID
	if err := tx.QueryRow(ctx, "SELECT id FROM observers WHERE public_key = $1", pkC).Scan(&idC); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetObserverLastIATA(ctx, idC); err != nil || got != "" {
		t.Errorf("GetObserverLastIATA(no packets) = %q, %v; want empty", got, err)
	}

	page, err := store.ListObservers(ctx, []string{"YYJ", "YYZ"}, "", "", "", "", "", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].IATA != "YYJ" {
		t.Errorf("ListObservers(YYJ,YYZ) = %+v; want only B in YYJ", page.Items)
	}
}
