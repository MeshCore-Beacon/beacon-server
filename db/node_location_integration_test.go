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
	f := func(v float64) *float64 { return &v }
	advert := "advert"
	for _, tc := range []struct {
		name       string
		lat, lon   *float64
		clear      bool
		wantLat    *float64
		wantLon    *float64
		wantSource *string
	}{
		{"omission preserves location", nil, nil, false, f(45), f(-75), &advert},
		{"explicit reset clears location", nil, nil, true, nil, nil, nil},
		{"omission preserves cleared location", nil, nil, false, nil, nil, nil},
		{"new coordinates restore location", f(46), f(-76), false, f(46), f(-76), &advert},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params.Latitude, params.Longitude, params.ClearLocation = tc.lat, tc.lon, tc.clear
			updatedID, err := store.UpsertNode(ctx, params, ingest.RadioSettings{})
			if err != nil || updatedID != id {
				t.Fatalf("node identity changed: %v", err)
			}
			assertNodeLocation(t, ctx, tx, []byte{0x97}, tc.wantLat, tc.wantLon, tc.wantSource)
		})
	}
	t.Run("new node with cleared location has no source", func(t *testing.T) {
		fresh := ingest.UpsertNodeParams{PublicKey: []byte{0x98}, NodeType: 2, Name: "cleared fixture", ClearLocation: true}
		if _, err := store.UpsertNode(ctx, fresh, ingest.RadioSettings{}); err != nil {
			t.Fatal(err)
		}
		assertNodeLocation(t, ctx, tx, []byte{0x98}, nil, nil, nil)
	})
}

func assertNodeLocation(t *testing.T, ctx context.Context, tx pgx.Tx, pubkey []byte, wantLat, wantLon *float64, wantSource *string) {
	t.Helper()
	var gotLat, gotLon *float64
	var gotSource *string
	if err := tx.QueryRow(ctx, "SELECT latitude, longitude, location_source FROM nodes WHERE public_key=$1", pubkey).Scan(&gotLat, &gotLon, &gotSource); err != nil {
		t.Fatal(err)
	}
	if !equalPtr(gotLat, wantLat) || !equalPtr(gotLon, wantLon) || !equalPtr(gotSource, wantSource) {
		t.Fatalf("location = (%v,%v,%v), want (%v,%v,%v)", deref(gotLat), deref(gotLon), deref(gotSource), deref(wantLat), deref(wantLon), deref(wantSource))
	}
}

func equalPtr[T comparable](a, b *T) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func deref[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}
