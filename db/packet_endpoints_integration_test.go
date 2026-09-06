// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type endpointQueryCounter struct {
	sqlc.DBTX
	calls int
}

func (q *endpointQueryCounter) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.calls++
	return q.DBTX.Query(ctx, sql, args...)
}
func (q *endpointQueryCounter) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	q.calls++
	return q.DBTX.QueryRow(ctx, sql, args...)
}

func TestPacketEndpointSnapshotsPostgres(t *testing.T) {
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
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"packets", "packet_observations", "observers", "transport_scopes", "nodes", "channel_messages"} {
		exec("CREATE TEMP TABLE " + table + " (LIKE public." + table + " INCLUDING ALL) ON COMMIT DROP")
	}
	// Also lets the unchanged Store run against the regression fixture before migration.
	exec("ALTER TABLE pg_temp.packet_observations ADD COLUMN IF NOT EXISTS resolved_endpoints JSONB")
	exec("CREATE TEMP SEQUENCE endpoint_observation_id START 1000")
	exec("ALTER TABLE pg_temp.packet_observations ALTER COLUMN id SET DEFAULT nextval('pg_temp.endpoint_observation_id')")
	observer1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	observer2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	key := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	name, earlier := "Captured 👋 companion", "Earlier companion"
	high := &api.ResolvedHop{Confidence: "high", Nodes: []api.ResolvedNode{{ID: nodeID, Name: &name, PublicKey: key}}}
	old := &api.ResolvedHop{Confidence: "high", Nodes: []api.ResolvedNode{{ID: nodeID, Name: &earlier, PublicKey: key}}}
	ambiguous := &api.ResolvedHop{Confidence: "ambiguous", Nodes: []api.ResolvedNode{high.Nodes[0], {ID: observer2, PublicKey: "bb"}}}
	none := &api.ResolvedHop{Confidence: "none", Nodes: []api.ResolvedNode{}}
	snapshot := func(source, destination *api.ResolvedHop) []byte {
		value, err := json.Marshal(struct {
			Source      *api.ResolvedHop `json:"source,omitempty"`
			Destination *api.ResolvedHop `json:"destination,omitempty"`
		}{source, destination})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	check := func(source, destination, wantSource, wantDestination *api.ResolvedHop) {
		t.Helper()
		if !reflect.DeepEqual(source, wantSource) || !reflect.DeepEqual(destination, wantDestination) {
			t.Error("historical packet lost endpoint snapshot")
		}
	}
	exec("INSERT INTO observers(id,public_key) VALUES ($1,'\\x01'),($2,'\\x02')", observer1, observer2)
	exec("INSERT INTO nodes(id,public_key,name,node_type) VALUES ($1,decode($2,'hex'),$3,1)", nodeID, key, name)
	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	exec(`INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,origin_pubkey,first_heard_at,last_heard_at)
SELECT decode(lpad(to_hex(i),64,'0'),'hex'),kind,0,1,'\x00','\x00',decode($2,'hex'),
 $1::timestamptz+i*interval '1 second',$1::timestamptz+i*interval '1 second'
FROM (VALUES (1,4),(2,2),(3,3),(4,4),(5,3)) v(i,kind)`, anchor, key)
	for _, obs := range []struct {
		id, packet int
		observer   uuid.UUID
		iata       string
		data       []byte
	}{
		{1, 1, observer1, "YYZ", snapshot(old, nil)},
		{2, 1, observer2, "YVR", snapshot(high, nil)},
		{3, 2, observer1, "YYZ", snapshot(ambiguous, none)},
		{4, 3, observer2, "YVR", snapshot(nil, nil)},
		{5, 4, observer1, "YYZ", nil},
	} {
		exec(`INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,source_broker,resolved_endpoints)
VALUES ($1,decode($2,'hex'),$3,$4,$5,0,1,0,'fixture',$6)`, obs.id, fmt.Sprintf("%064x", obs.packet), obs.observer, obs.iata, anchor.Add(time.Duration(obs.id)*time.Second), obs.data)
	}
	exec("UPDATE nodes SET name='Current name'")
	q := &endpointQueryCounter{DBTX: tx}
	store := &Store{q: sqlc.New(q)}
	for _, iatas := range [][]string{nil, {"YYZ"}, {"YVR"}, {"YYZ", "YVR", "YYZ"}} {
		q.calls = 0
		page, err := store.ListPackets(ctx, nil, nil, iatas, nil, time.Time{}, time.Time{}, 0, 50)
		if err != nil {
			t.Fatal(err)
		}
		if q.calls != 1 || len(page.Items) == 0 {
			t.Fatalf("list used %d queries or lost its page", q.calls)
		}
		for _, item := range page.Items {
			switch item.PacketHash {
			case fmt.Sprintf("%064x", 1):
				// The existing regional roll-up also selects the global latest observer.
				if item.LatestObserver == nil || item.LatestObserver.ID != observer2 || item.LatestObserver.IATA != "YVR" {
					t.Fatal("latest observer selection changed")
				}
				check(item.LatestObserver.ResolvedSource, item.LatestObserver.ResolvedDestination, high, nil)
			case fmt.Sprintf("%064x", 2):
				check(item.LatestObserver.ResolvedSource, item.LatestObserver.ResolvedDestination, ambiguous, none)
			case fmt.Sprintf("%064x", 3), fmt.Sprintf("%064x", 4):
				check(item.LatestObserver.ResolvedSource, item.LatestObserver.ResolvedDestination, nil, nil)
			case fmt.Sprintf("%064x", 5):
				if item.LatestObserver != nil {
					t.Fatal("invented an observation for a packet without one")
				}
			}
		}
	}
	q.calls = 0
	backfill, err := store.ListPacketsAfterID(ctx, 0, -1, -1, nil, "", 50)
	if err != nil || len(backfill) != 5 || q.calls != 1 {
		t.Fatalf("backfill: rows=%d queries=%d err=%v", len(backfill), q.calls, err)
	}
	check(backfill[0].LatestObserver.ResolvedSource, backfill[0].LatestObserver.ResolvedDestination, old, nil)
	check(backfill[1].LatestObserver.ResolvedSource, backfill[1].LatestObserver.ResolvedDestination, high, nil)
	check(backfill[2].LatestObserver.ResolvedSource, backfill[2].LatestObserver.ResolvedDestination, ambiguous, none)
	packet1 := append(make([]byte, 31), 1)
	for pass := 0; pass < 2; pass++ {
		q.calls = 0
		packet, err := store.GetPacket(ctx, packet1)
		if err != nil || len(packet.Observations) != 2 {
			t.Fatalf("packet detail: %v", err)
		}
		check(packet.Observations[0].ResolvedSource, packet.Observations[0].ResolvedDestination, old, nil)
		check(packet.Observations[1].ResolvedSource, packet.Observations[1].ResolvedDestination, high, nil)
		if q.calls != 2 {
			t.Errorf("snapshot detail used %d queries, want packet plus observations only", q.calls)
		}
		if pass == 0 {
			legacy, err := store.GetPacket(ctx, append(make([]byte, 31), 4))
			if err != nil || legacy.Observations[0].ResolvedSource == nil || *legacy.Observations[0].ResolvedSource.Nodes[0].Name != "Current name" {
				t.Fatalf("legacy detail lookup changed: %v", err)
			}
			exec("DELETE FROM nodes")
		}
	}
	exec(`INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
SELECT decode(lpad(to_hex(i),64,'0'),'hex'),4,0,1,'\x00','\x00',$1::timestamptz+i*interval '1 second',$1::timestamptz+i*interval '1 second'
FROM generate_series(6,55) i`, anchor)
	exec(`INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,source_broker,resolved_endpoints)
SELECT i,decode(lpad(to_hex(i),64,'0'),'hex'),$1,'YYZ',$2::timestamptz+i*interval '1 second',0,1,0,'fixture',$3
FROM generate_series(6,55) i`, observer1, anchor, snapshot(high, nil))
	q.calls = 0
	page, err := store.ListPackets(ctx, nil, nil, nil, nil, time.Time{}, time.Time{}, 0, 50)
	if err != nil || len(page.Items) != 50 || q.calls != 1 {
		t.Fatalf("50-row page: queries=%d err=%v", q.calls, err)
	}
	for _, item := range page.Items {
		check(item.LatestObserver.ResolvedSource, item.LatestObserver.ResolvedDestination, high, nil)
	}
	t.Logf("50 historical rows use %d database query", q.calls)

	// Capture uses the ordinary insert; duplicate delivery cannot replace its snapshot.
	exec(`INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
VALUES (decode($1,'hex'),4,0,1,'\x00','\x00',NOW(),NOW())`, fmt.Sprintf("%064x", 56))
	observation := ingest.InsertObservationParams{PacketHash: append(make([]byte, 31), 56),
		ObserverID: observer1, IATA: "YYZ", HeardAt: anchor, HashSize: 1, SourceBroker: "fixture",
		PayloadType: 4, ResolvedEndpoints: snapshot(high, none)}
	for _, wantInserted := range []bool{true, false} {
		q.calls = 0
		inserted, err := store.InsertObservation(ctx, observation)
		if err != nil || inserted != wantInserted || q.calls != 1 {
			t.Fatalf("snapshot insert: inserted=%v queries=%d err=%v", inserted, q.calls, err)
		}
		observation.ResolvedEndpoints = snapshot(old, nil)
		observation.IATA = "YVR"
	}
	stored, err := store.GetPacket(ctx, observation.PacketHash)
	if err != nil || len(stored.Observations) != 1 || stored.Observations[0].IATA != "YYZ" {
		t.Fatalf("duplicate observation changed: %v", err)
	}
	check(stored.Observations[0].ResolvedSource, stored.Observations[0].ResolvedDestination, high, none)
	// An empty capture must not turn into a later registry lookup.
	exec("UPDATE packet_observations SET resolved_endpoints='{}',source_broker=NULL WHERE packet_hash=$1", observation.PacketHash)
	q.calls = 0
	stored, err = store.GetPacket(ctx, observation.PacketHash)
	if err != nil || q.calls != 2 || stored.Observations[0].SourceBroker != "" {
		t.Fatalf("empty snapshot or nullable legacy broker: queries=%d err=%v", q.calls, err)
	}
	check(stored.Observations[0].ResolvedSource, stored.Observations[0].ResolvedDestination, nil, nil)
	empty, err := store.GetPacket(ctx, append(make([]byte, 31), 5))
	if err != nil || len(empty.Observations) != 0 {
		t.Fatalf("packet awaiting its first observation: %v", err)
	}
}

func TestDecodePacketEndpointSnapshot(t *testing.T) {
	for _, tc := range []struct {
		raw      string
		captured bool
	}{
		{"", false}, {"null", false}, {"[", false}, {"[]", false}, {"{}", true},
		{`{"source":{"confidence":"none","nodes":[]}}`, true},
	} {
		_, captured := decodePacketEndpointSnapshot(json.RawMessage(tc.raw))
		if captured != tc.captured {
			t.Errorf("snapshot %q: captured=%v, want %v", tc.raw, captured, tc.captured)
		}
	}
}
