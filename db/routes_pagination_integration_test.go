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
	"github.com/jackc/pgx/v5"
)

// Routes upserted in one batch share last_seen; paging must not skip any of them.
func TestKnownRoutePaginationTiesPostgres(t *testing.T) {
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
	applyBaseline(t, ctx, tx)
	// Seven routes in one millisecond (two with sub-ms offsets), one older, one in another IATA.
	if _, err := tx.Exec(ctx, `
INSERT INTO iata_codes (iata) VALUES ('YYZ'), ('YOW') ON CONFLICT DO NOTHING;
INSERT INTO known_routes (id,path_key,node_ids,hash_prefix,iata,hop_count,first_seen,last_seen)
SELECT i, decode(md5(i::text),'hex'), ARRAY[]::uuid[], ARRAY['\x01'::bytea],
       CASE WHEN i = 9 THEN 'YOW' ELSE 'YYZ' END, 2,
       '2026-01-01 00:00:00+00'::timestamptz,
       CASE WHEN i = 8 THEN '2026-01-01 00:00:04+00'::timestamptz
            WHEN i IN (3, 6) THEN '2026-01-01 00:00:05.000400+00'::timestamptz
            ELSE '2026-01-01 00:00:05+00'::timestamptz END
FROM generate_series(1,9) i;`); err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	for _, iata := range []string{"", "YYZ"} {
		t.Run("iata="+iata, func(t *testing.T) {
			var got []int64
			var cursor time.Time
			var cursorID int64
			for page := 0; page < 10; page++ {
				routes, err := store.ListKnownRoutes(ctx, iata, 0, cursor, cursorID, 3)
				if err != nil {
					t.Fatal(err)
				}
				if len(routes) == 0 {
					break
				}
				for _, r := range routes {
					got = append(got, r.ID)
				}
				last := routes[len(routes)-1]
				cursor, cursorID = time.UnixMilli(last.LastSeen), last.ID
			}
			want := []int64{7, 6, 5, 4, 3, 2, 1, 8}
			if iata == "" {
				want = []int64{9, 7, 6, 5, 4, 3, 2, 1, 8}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("paged ids = %v, want %v", got, want)
			}
		})
	}
}
