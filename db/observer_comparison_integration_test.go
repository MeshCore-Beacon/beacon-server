// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestObserverComparisonPostgres(t *testing.T) {
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
	// Temporary tables shadow the private database's tables; no retained rows
	// or sequences are changed, and the fixture is rolled back on every exit.
	for _, table := range []string{"observers", "packets", "packet_observations"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	_, err = tx.Exec(ctx, `
INSERT INTO observers (id, public_key)
SELECT ('00000000-0000-0000-0000-' || lpad(i::text,12,'0'))::uuid,
       decode(lpad(to_hex(i),64,'0'),'hex') FROM generate_series(1,3) i;
INSERT INTO packets (packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
SELECT decode(lpad(to_hex(i),2,'0'),'hex'),4,0,route,'\x00','\x00',NOW(),NOW()
FROM (VALUES (1,1),(2,0),(3,1),(4,1),(5,0),(6,1),(7,2),(8,3),(9,1),(10,1)) v(i,route);
INSERT INTO packet_observations (id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,source_broker)
SELECT id,decode(lpad(to_hex(packet),2,'0'),'hex'),
       ('00000000-0000-0000-0000-' || lpad(observer::text,12,'0'))::uuid,
       iata,'2026-01-01'::timestamptz+second*interval '1 second',0,1,0,broker
FROM (VALUES
 (1,1,1,'YVR',1,'a'),(2,1,1,'YVR',2,'b'),
 (3,2,2,'YVR',2,'a'),(4,3,1,'YVR',3,'a'),(5,3,2,'YYJ',4,'a'),
 (6,4,1,'YVR',-1,'a'),(7,4,2,'YVR',5,'a'),(8,5,1,'YVR',0,'a'),
 (9,6,2,'YVR',10,'a'),(10,7,1,'YVR',1,'a'),(11,7,2,'YVR',2,'a'),
 (12,8,2,'YVR',1,'a'),(13,9,3,'YVR',1,'a'),
 (14,10,1,'YVR',3,'a'),(15,10,2,'YVR',9,'a')) v(id,packet,observer,iata,second,broker);
`)
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	a := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	b := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		iatas    []string
		from, to int
		reverse  bool
		counts   [4]int64 // union, only A, only B, both
	}{
		{"global distinct floods", nil, 0, 10, false, [4]int64{6, 2, 2, 2}},
		{"empty filter", []string{}, 0, 10, false, [4]int64{6, 2, 2, 2}},
		{"YVR receptions", []string{"YVR"}, 0, 10, false, [4]int64{6, 3, 2, 1}},
		{"reversed observers", []string{"YVR"}, 0, 10, true, [4]int64{6, 2, 3, 1}},
		{"YYJ receptions", []string{"YYJ"}, 0, 10, false, [4]int64{1, 0, 1, 0}},
		{"region union", []string{"YYJ", "YVR", "YYJ"}, 0, 10, false, [4]int64{6, 2, 2, 2}},
		{"unknown IATA", []string{"ZZZ"}, 0, 10, false, [4]int64{}},
		{"inclusive start exclusive end", nil, 0, 1, false, [4]int64{1, 1, 0, 0}},
		{"next interval", nil, 1, 3, false, [4]int64{2, 1, 1, 0}},
		{"known observers empty interval", nil, 20, 30, false, [4]int64{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first, second := a, b
			if tc.reverse {
				first, second = b, a
			}
			since, until := start.Add(time.Duration(tc.from)*time.Second), start.Add(time.Duration(tc.to)*time.Second)
			got, err := store.GetObserverComparison(ctx, first, second, since, until, tc.iatas)
			if err != nil {
				t.Fatal(err)
			}
			if got.ObserverA != first || got.ObserverB != second || got.Since != since.UnixMilli() || got.Until != until.UnixMilli() {
				t.Fatalf("comparison context = %+v", got)
			}
			if counts := [4]int64{got.TotalPackets, got.OnlyA, got.OnlyB, got.Both}; counts != tc.counts {
				t.Fatalf("counts = %v; want %v", counts, tc.counts)
			}
		})
	}
	if _, err := store.GetObserverComparison(ctx, a, uuid.New(), start, start.Add(time.Hour), nil); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("unknown observer: %v", err)
	}
}
