// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
)

// Run on a migrated private database. All fixture tables and writes are rolled back.
func TestScopeStatsPostgres(t *testing.T) {
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
	for _, table := range []string{"transport_scopes", "packets", "packet_observations", "observer_scopes", "nodes", "node_iatas"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	_, err = tx.Exec(ctx, `
INSERT INTO transport_scopes (id,name,transport_key,key_fingerprint)
SELECT i,name,decode(repeat('00',16),'hex'),decode(lpad(to_hex(i),16,'0'),'hex')
FROM (VALUES (1,'#a'),(2,'#b'),(3,'#unused')) v(i,name);
INSERT INTO packets (packet_hash,scope_id,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
SELECT decode(lpad(to_hex(i),2,'0'),'hex'),scope_id,4,0,1,'\x00','\x00',NOW(),NOW()
FROM (VALUES (1,1),(2,1),(3,1),(4,1),(5,2),(6,2),(7,2),(8,NULL)) v(i,scope_id);
INSERT INTO observer_scopes (observer_id,scope_id)
SELECT md5(i::text)::uuid,scope_id FROM (VALUES (1,1),(2,1),(4,1),(5,1),(1,2),(2,2),(3,2)) v(i,scope_id);
INSERT INTO packet_observations (id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
SELECT id,decode(lpad(to_hex(packet),2,'0'),'hex'),md5(observer::text)::uuid,iata,
 '2026-01-01'::timestamptz+id*interval '1 second',0,1,0
FROM (VALUES (1,1,1,'YVR'),(2,1,4,'YVR'),(3,1,2,'YYJ'),(4,2,2,'YVR'),
 (5,3,1,'YYZ'),(6,5,3,'YVR'),(7,6,1,'YYJ'),(8,7,2,'YYZ'),(9,8,1,'YVR')) v(id,packet,observer,iata);
INSERT INTO nodes (id,public_key,node_type,default_scope_id)
SELECT md5(i::text)::uuid,decode(lpad(to_hex(i),64,'0'),'hex'),2,scope_id
FROM (VALUES (1,1),(2,1),(3,1),(4,1),(5,2),(6,2),(7,NULL)) v(i,scope_id);
INSERT INTO node_iatas (node_id,iata)
SELECT md5(i::text)::uuid,iata FROM (VALUES (1,'YVR'),(1,'YYJ'),(2,'YVR'),(3,'YYZ'),(5,'YYJ'),(6,'YVR'),(6,'YYJ'),(7,'YVR')) v(i,iata);
`)
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	for _, tc := range []struct {
		name  string
		iatas []string
		a, b  [3]int64 // packets, observers, nodes
	}{
		{"global", nil, [3]int64{4, 4, 4}, [3]int64{3, 3, 2}},
		{"empty filter", []string{}, [3]int64{4, 4, 4}, [3]int64{3, 3, 2}},
		{"YVR", []string{"YVR"}, [3]int64{2, 3, 2}, [3]int64{1, 1, 1}},
		{"YYJ", []string{"YYJ"}, [3]int64{1, 1, 1}, [3]int64{1, 1, 2}},
		{"YYZ", []string{"YYZ"}, [3]int64{1, 1, 1}, [3]int64{1, 1, 0}},
		{"overlapping IATAs", []string{"YVR", "YYJ"}, [3]int64{2, 3, 2}, [3]int64{2, 2, 2}},
		{"duplicate IATAs", []string{"YYJ", "YVR", "YYJ"}, [3]int64{2, 3, 2}, [3]int64{2, 2, 2}},
		{"unknown IATA", []string{"ZZZ"}, [3]int64{}, [3]int64{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := store.GetScopeStats(ctx, tc.iatas)
			if err != nil {
				t.Fatal(err)
			}
			want := []api.ScopeStats{
				{Name: "#a", PacketCount: tc.a[0], ObserverCount: tc.a[1], NodeCount: tc.a[2]},
				{Name: "#b", PacketCount: tc.b[0], ObserverCount: tc.b[1], NodeCount: tc.b[2]},
				{Name: "#unused"},
			}
			if !reflect.DeepEqual(rows, want) {
				t.Fatalf("counts = %+v, want %+v", rows, want)
			}
		})
	}
	if _, err := tx.Exec(ctx, "DELETE FROM transport_scopes"); err != nil {
		t.Fatal(err)
	}
	rows, err := store.GetScopeStats(ctx, nil)
	if err != nil || rows == nil || len(rows) != 0 {
		t.Fatalf("empty roster = %+v, %v", rows, err)
	}
}
