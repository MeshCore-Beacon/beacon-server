// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
)

type summaryQueryCounter struct {
	pgx.Tx
	calls int
}

func (c *summaryQueryCounter) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	c.calls++
	return c.Tx.Query(ctx, query, args...)
}

func TestPacketSummariesPostgres(t *testing.T) {
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
	for _, table := range []string{"packets", "packet_observations", "observers", "transport_scopes"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	_, err = tx.Exec(ctx, `
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,parsed_payload,first_heard_at,last_heard_at)
SELECT decode(lpad(to_hex(id),2,'0'),'hex'),kind,0,1,'\x00','\x00',parsed::jsonb,
 '2026-01-01'::timestamptz+id*interval '1 second','2026-01-01'::timestamptz+id*interval '1 second'
FROM (VALUES
 (1,4,'{"type":"ADVERT","appData":{"name":"MD00-Repeater"}}'),
 (2,4,'{"type":"ADVERT","appData":{"name":"Relay 📡"}}'),
 (3,4,'{"type":"ADVERT","appData":{"name":""}}'),
 (4,4,'{"type":"ADVERT","appData":{}}'),
 (5,4,NULL),
 (6,4,'{"type":"ADVERT","appData":{"name":null}}'),
 (7,4,'{"type":"ADVERT","appData":{"name":42}}'),
 (8,2,'{"appData":{"name":"not an advert"}}'),
 (9,4,'{"type":"ADVERT","appData":[]}')
) v(id,kind,parsed);
`)
	if err != nil {
		t.Fatal(err)
	}
	counter := &summaryQueryCounter{Tx: tx}
	store := &Store{q: sqlc.New(counter)}
	check := func(items []api.PacketSummary) {
		t.Helper()
		if len(items) != 9 {
			t.Fatalf("got %d packets, want 9", len(items))
		}
		for _, item := range items {
			want := map[string]string{"01": "MD00-Repeater", "02": "Relay 📡"}[item.PacketHash]
			if want == "" {
				if item.Summary != nil {
					t.Fatalf("unexpected summary for %s: %q", item.PacketHash, *item.Summary)
				}
			} else if item.Summary == nil || *item.Summary != want {
				t.Fatalf("summary for %s = %v, want %q", item.PacketHash, item.Summary, want)
			}
		}
		if counter.calls != 1 {
			t.Fatalf("list made %d queries, want 1", counter.calls)
		}
		counter.calls = 0
	}
	page, err := store.ListPackets(ctx, nil, nil, nil, nil, time.Time{}, time.Time{}, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	check(page.Items) // names remain available before any observation arrives
	_, err = tx.Exec(ctx, `
INSERT INTO observers(id,public_key) VALUES ('00000000-0000-0000-0000-000000000001','\x01');
INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
SELECT id,decode(lpad(to_hex(id),2,'0'),'hex'),'00000000-0000-0000-0000-000000000001','YVR',
 '2026-01-01'::timestamptz+id*interval '1 second',0,1,0 FROM generate_series(1,9) id;
`)
	if err != nil {
		t.Fatal(err)
	}
	for _, iatas := range [][]string{nil, {"YVR"}} {
		page, err := store.ListPackets(ctx, nil, nil, iatas, nil, time.Time{}, time.Time{}, 0, 20)
		if err != nil {
			t.Fatal(err)
		}
		check(page.Items)
		rows, err := store.ListPacketsAfterID(ctx, 0, -1, -1, iatas, "", 20)
		if err != nil {
			t.Fatal(err)
		}
		check(rows)
	}
}
