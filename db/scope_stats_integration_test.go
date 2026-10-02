// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"reflect"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
)

// Packets come from the rolled IATA sets (each packet once per hour, however many requested
// IATAs heard it); observers are current memberships filtered by their latest IATA.
func TestScopeStatsPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var h time.Time
	if err := pool.QueryRow(ctx, "SELECT date_trunc('hour', now(), 'UTC') - INTERVAL '4 hours'").Scan(&h); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO iata_codes (iata) VALUES ('YVR'), ('YYJ'), ('YYZ');
INSERT INTO transport_scopes (id,name,transport_key,key_fingerprint)
SELECT i,name,decode(repeat('00',16),'hex'),decode(lpad(to_hex(i),16,'0'),'hex')
FROM (VALUES (1,'#a'),(2,'#b'),(3,'#unused')) v(i,name);
INSERT INTO observers (id,public_key,last_iata)
SELECT md5(i::text)::uuid,decode(lpad(to_hex(i),2,'0'),'hex'),iata
FROM (VALUES (1,'YVR'),(2,'YYJ'),(3,'YVR'),(4,'YVR'),(5,'YYZ')) v(i,iata);
-- every packet is an advert from its own node, so per-scope nodes equal per-scope packets
INSERT INTO packets (packet_hash,scope_id,payload_type,payload_version,route_type,origin_pubkey,raw_payload,raw_header,first_heard_at,last_heard_at)
SELECT decode(lpad(to_hex(i),2,'0'),'hex'),scope_id,4,0,1,decode(lpad(to_hex(100+i),64,'0'),'hex'),'\x00','\x00',$1::timestamptz,$1::timestamptz
FROM (VALUES (1,1),(2,1),(3,1),(4,1),(5,2),(6,2),(7,2),(8,NULL)) v(i,scope_id);
INSERT INTO observer_scopes (observer_id,scope_id)
SELECT md5(i::text)::uuid,scope_id FROM (VALUES (1,1),(2,1),(4,1),(5,1),(1,2),(2,2),(3,2)) v(i,scope_id);
-- packet 4 is never heard, so it never reaches the rollup
INSERT INTO packet_observations (id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type)
SELECT id,decode(lpad(to_hex(packet),2,'0'),'hex'),md5(observer::text)::uuid,iata,
 $1::timestamptz+id*interval '1 second',0,1,0,4
FROM (VALUES (1,1,1,'YVR'),(2,1,4,'YVR'),(3,1,2,'YYJ'),(4,2,2,'YVR'),
 (5,3,1,'YYZ'),(6,5,3,'YVR'),(7,6,1,'YYJ'),(8,7,2,'YYZ'),(9,8,1,'YVR')) v(id,packet,observer,iata);
INSERT INTO nodes (id,public_key,node_type,default_scope_id)
SELECT md5(i::text)::uuid,decode(lpad(to_hex(i),64,'0'),'hex'),2,scope_id
FROM (VALUES (1,1),(2,1),(3,1),(4,1),(5,2),(6,2),(7,NULL)) v(i,scope_id);
INSERT INTO node_iatas (node_id,iata)
SELECT md5(i::text)::uuid,iata FROM (VALUES (1,'YVR'),(1,'YYJ'),(2,'YVR'),(3,'YYZ'),(5,'YYJ'),(6,'YVR'),(6,'YYJ'),(7,'YVR')) v(i,iata);
`, pgx.QueryExecModeSimpleProtocol, h); err != nil {
		t.Fatal(err)
	}
	store := New(pool, 0, 0)
	r, ok, err := store.BeginRollup(ctx)
	if err != nil || !ok {
		t.Fatalf("BeginRollup: ok=%v err=%v", ok, err)
	}
	if err := r.RegisterHours(ctx, h); err != nil {
		t.Fatal(err)
	}
	rollAll(t, ctx, r)
	r.Close(ctx)

	for _, tc := range []struct {
		name   string
		iatas  []string
		since  time.Time
		a, b   [3]int64 // packets, observers, nodes
		ah, bh int64    // distinct observers heard carrying the scope in hour h
	}{
		{"global", nil, time.Time{}, [3]int64{3, 4, 4}, [3]int64{3, 3, 2}, 3, 3},
		{"empty filter", []string{}, time.Time{}, [3]int64{3, 4, 4}, [3]int64{3, 3, 2}, 3, 3},
		{"YVR", []string{"YVR"}, time.Time{}, [3]int64{2, 2, 2}, [3]int64{1, 2, 1}, 3, 1},
		{"YYJ", []string{"YYJ"}, time.Time{}, [3]int64{1, 1, 1}, [3]int64{1, 1, 2}, 1, 1},
		{"YYZ", []string{"YYZ"}, time.Time{}, [3]int64{1, 1, 1}, [3]int64{1, 0, 0}, 1, 1},
		{"overlapping IATAs", []string{"YVR", "YYJ"}, time.Time{}, [3]int64{2, 3, 2}, [3]int64{2, 3, 2}, 3, 2},
		{"duplicate IATAs", []string{"YYJ", "YVR", "YYJ"}, time.Time{}, [3]int64{2, 3, 2}, [3]int64{2, 3, 2}, 3, 2},
		{"unknown IATA", []string{"ZZZ"}, time.Time{}, [3]int64{}, [3]int64{}, 0, 0},
		{"window after the traffic", nil, h.Add(time.Hour), [3]int64{0, 4, 4}, [3]int64{0, 3, 2}, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := store.GetScopeStats(ctx, tc.iatas, tc.since)
			if err != nil {
				t.Fatal(err)
			}
			// All fixture traffic is in hour h; hours with no packets are omitted.
			// Each packet has its own origin, so active nodes per hour equal packets per hour.
			hourly := func(packets, observers int64) []api.ScopeHour {
				if packets == 0 && observers == 0 {
					return []api.ScopeHour{}
				}
				return []api.ScopeHour{{Hour: h.UnixMilli(), Packets: packets, Observers: observers, Nodes: packets}}
			}
			want := []api.ScopeStats{
				{Name: "#a", PacketCount: tc.a[0], ObserverCount: tc.a[1], NodeCount: tc.a[2], Hourly: hourly(tc.a[0], tc.ah)},
				{Name: "#b", PacketCount: tc.b[0], ObserverCount: tc.b[1], NodeCount: tc.b[2], Hourly: hourly(tc.b[0], tc.bh)},
				{Name: "#unused", Hourly: []api.ScopeHour{}},
			}
			if !reflect.DeepEqual(rows, want) {
				t.Fatalf("counts = %+v, want %+v", rows, want)
			}
		})
	}

	detail, err := store.GetScopeByName(ctx, "#a")
	if err != nil {
		t.Fatal(err)
	}
	want := &api.ScopeDetail{Name: "#a", PacketCount: 3, ObserverCount: 4, NodeCount: 4, IATACount: 3, IATAs: []string{"YVR", "YYJ", "YYZ"}}
	if !reflect.DeepEqual(detail, want) {
		t.Errorf("scope detail %+v, want %+v", detail, want)
	}

	if _, err := pool.Exec(ctx, "TRUNCATE transport_scopes CASCADE"); err != nil {
		t.Fatal(err)
	}
	rows, err := store.GetScopeStats(ctx, nil, time.Time{})
	if err != nil || rows == nil || len(rows) != 0 {
		t.Fatalf("empty roster = %+v, %v", rows, err)
	}
}
