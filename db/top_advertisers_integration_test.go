// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
)

func TestTopAdvertisersSortPostgres(t *testing.T) {
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

	// a1 is a chatty companion (mostly direct); a3 ties a2 on flood and wins on total.
	if _, err := tx.Exec(ctx, `
INSERT INTO analytics_hourly_advert_sets (hour, origin_pubkey, iatas, advert_packets, flood_packets, direct_packets)
SELECT date_trunc('hour', now(), 'UTC') - INTERVAL '2 hours', pk, '{YVR}', f + d, f, d FROM (VALUES
 ('\xa1'::bytea, 10, 8300), ('\xa2', 50, 0), ('\xa3', 50, 5)
) v(pk, f, d)`); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		sort  api.AdvertiserSort
		limit int32
		want  []string
	}{
		{api.AdvertiserSortFlood, 1, []string{"a3"}},
		{api.AdvertiserSortFlood, 3, []string{"a3", "a2", "a1"}},
		{api.AdvertiserSortDirect, 1, []string{"a1"}},
		{api.AdvertiserSortDirect, 3, []string{"a1", "a3", "a2"}},
	} {
		rows, err := store.GetStatsTopAdvertisers(ctx, []string{"YVR"}, time.Time{}, tc.limit, tc.sort)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]string, len(rows))
		for i, r := range rows {
			got[i] = r.PublicKey
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("sort=%s limit=%d: got %v, want %v", tc.sort, tc.limit, got, tc.want)
		}
	}
}
