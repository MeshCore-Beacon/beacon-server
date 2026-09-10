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

func TestNodeLocationResetPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	if _, err := tx.Exec(ctx, "CREATE TEMP TABLE nodes (LIKE public.nodes INCLUDING ALL) ON COMMIT DROP"); err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	lat, lon := 45.0, -75.0
	params := ingest.UpsertNodeParams{PublicKey: []byte{0x97}, NodeType: 2, Name: "location fixture", Latitude: &lat, Longitude: &lon}
	id, err := store.UpsertNode(ctx, params, ingest.RadioSettings{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		present  bool
		lat, lon float64
	}{
		{"omission preserves location", false, 45, -75},
		{"explicit reset replaces location", true, 0, 0},
		{"omission preserves reset", false, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params.Latitude, params.Longitude = nil, nil
			if tc.present {
				params.Latitude, params.Longitude = &tc.lat, &tc.lon
			}
			updatedID, err := store.UpsertNode(ctx, params, ingest.RadioSettings{})
			if err != nil || updatedID != id {
				t.Fatalf("node identity changed: %v", err)
			}
			var gotLat, gotLon float64
			if err := tx.QueryRow(ctx, "SELECT latitude,longitude FROM nodes WHERE id=$1", id).Scan(&gotLat, &gotLon); err != nil {
				t.Fatal(err)
			}
			if gotLat != tc.lat || gotLon != tc.lon {
				t.Fatalf("location = (%g,%g), want (%g,%g)", gotLat, gotLon, tc.lat, tc.lon)
			}
		})
	}
}
