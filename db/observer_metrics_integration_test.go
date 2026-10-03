// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package db

import (
	"encoding/json"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"testing"
	"time"
)

// Hourly activity reads the rollup, but the last ~2h aren't rolled yet; they come from raw rows.
func TestObserverActivityUnrolledTailPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	now := time.Now().UTC()
	rolled := now.Truncate(time.Hour).Add(-5 * time.Hour)
	if _, err := tx.Exec(ctx, `
 INSERT INTO observers (id,public_key) VALUES ('00000000-0000-0000-0000-000000000001','\x01');
 INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,last_heard_at,first_heard_at) SELECT int4send(i),4,0,1,'\x00','\x00',NOW(),NOW() FROM generate_series(1,4) i;`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
 INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type,snr,rssi) VALUES
  (int4send(1),'00000000-0000-0000-0000-000000000001','YOW',$1::timestamptz+interval '10 minutes',0,1,0,4,5,-90),
  (int4send(2),'00000000-0000-0000-0000-000000000001','YOW',$1::timestamptz+interval '20 minutes',0,1,0,4,7,-80);`, rolled); err != nil {
		t.Fatal(err)
	}
	rollTxHours(t, ctx, tx)
	// Heard since the newest rolled hour; the rollup hasn't reached them.
	if _, err := tx.Exec(ctx, `
 INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type,snr,rssi) VALUES
  (int4send(3),'00000000-0000-0000-0000-000000000001','YOW',$1::timestamptz-interval '100 minutes',0,1,0,4,9,-70),
  (int4send(4),'00000000-0000-0000-0000-000000000001','YOW',$1::timestamptz-interval '5 minutes',0,1,0,NULL,0,0);`, now); err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	id := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	activity, err := store.GetObserverActivity(ctx, id, 24*time.Hour, time.Hour, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if activity.Summary.RecordedPackets != 4 {
		t.Errorf("recorded packets = %d, want 4 (2 rolled + 2 unrolled)", activity.Summary.RecordedPackets)
	}
	byHour := map[int64]int64{}
	for _, p := range activity.Points {
		byHour[p.T] = p.Observations
	}
	recent := now.Add(-100 * time.Minute).Truncate(time.Hour).UnixMilli()
	if byHour[rolled.UnixMilli()] != 2 || byHour[recent] != 1 || byHour[now.Add(-5*time.Minute).Truncate(time.Hour).UnixMilli()] != 1 {
		t.Errorf("points %+v", activity.Points)
	}
	for _, p := range activity.Points {
		if p.T == recent && (p.SNRAvg == nil || *p.SNRAvg != 9 || p.RSSIAvg == nil || *p.RSSIAvg != -70) {
			t.Errorf("tail signal: %+v", p)
		}
	}
	types := map[int16]int64{}
	for _, v := range activity.PayloadTypes {
		types[v.PayloadType] = v.Count
	}
	if types[4] != 3 || types[-1] != 1 {
		t.Errorf("payload types %+v", activity.PayloadTypes)
	}
}

// A rollup backlog longer than the usual lag must still be read from raw rows.
func TestObserverActivityRollupBacklogPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	now := time.Now().UTC()
	rolled := now.Truncate(time.Hour).Add(-10 * time.Hour)
	if _, err := tx.Exec(ctx, `
 INSERT INTO observers (id,public_key) VALUES ('00000000-0000-0000-0000-000000000001','\x01');
 INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,last_heard_at,first_heard_at) SELECT int4send(i),4,0,1,'\x00','\x00',NOW(),NOW() FROM generate_series(1,3) i;`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
 INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type) VALUES
  (int4send(1),'00000000-0000-0000-0000-000000000001','YOW',$1::timestamptz+interval '10 minutes',0,1,0,4);`, rolled); err != nil {
		t.Fatal(err)
	}
	rollTxHours(t, ctx, tx)
	if _, err := tx.Exec(ctx, `
 INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type) VALUES
  (int4send(2),'00000000-0000-0000-0000-000000000001','YOW',$1::timestamptz-interval '8 hours',0,1,0,4),
  (int4send(3),'00000000-0000-0000-0000-000000000001','YOW',$1::timestamptz-interval '6 hours',0,1,0,4);`, now); err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	id := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	activity, err := store.GetObserverActivity(ctx, id, 24*time.Hour, time.Hour, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if activity.Summary.RecordedPackets != 3 {
		t.Errorf("recorded packets = %d, want 3 (1 rolled + 2 in the backlog)", activity.Summary.RecordedPackets)
	}
	wantRolled := rolled.Add(time.Hour).UnixMilli()
	if activity.RolledUntil == nil || *activity.RolledUntil != wantRolled {
		t.Errorf("rolledUntil = %v, want %d", activity.RolledUntil, wantRolled)
	}
	if activity.RawFrom == nil || *activity.RawFrom != wantRolled {
		t.Errorf("rawFrom = %v, want %d (no gap)", activity.RawFrom, wantRolled)
	}
}

func TestObserverMetricsPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	if _, err := tx.Exec(ctx, `
 INSERT INTO observers (id,public_key) VALUES ('00000000-0000-0000-0000-000000000001','\x01');
 INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,last_heard_at,first_heard_at) SELECT int4send(i),4,0,1,'\x00','\x00',NOW()-interval '5 days',NOW()-interval '5 days' FROM generate_series(1,3) i;
 INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type,snr,rssi)
 SELECT packet_hash,'00000000-0000-0000-0000-000000000001','YOW',date_trunc('hour',NOW())-interval '5 days',0,1,0,CASE WHEN packet_hash=int4send(1) THEN 4 END,CASE WHEN packet_hash IN (int4send(1),int4send(2)) THEN 'NaN'::real ELSE 0 END,-100 FROM packets;
 `); err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	id := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	// Hourly activity is rolled before cleanup and survives the raw rows.
	rollTxHours(t, ctx, tx)
	if err := store.DeleteOldPackets(ctx, time.Now().Add(-72*time.Hour)); err != nil {
		t.Fatal(err)
	}
	until := time.Now().UTC().Truncate(time.Hour)
	activity, err := store.GetObserverActivity(ctx, id, 7*24*time.Hour, time.Hour, until)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := json.Marshal(activity); err != nil {
		t.Fatalf("non-finite sample leaked into JSON: %v", err)
	}
	if activity.Summary.RecordedPackets != 3 || len(activity.PayloadTypes) != 2 || activity.Summary.LatestRecordedAt != nil || activity.Summary.LastCompleteHour != 0 {
		t.Fatalf("archive summary: %+v types=%+v", activity.Summary, activity.PayloadTypes)
	}
	if activity.WindowEnd != until.UnixMilli() || activity.WindowStart != until.Add(-7*24*time.Hour).UnixMilli() || activity.Source != "hourly" {
		t.Fatalf("window: %+v", activity)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at) VALUES ('\xaa',4,0,1,'\x00','\x00',NOW(),NOW());
 INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,snr,rssi) VALUES ('\xaa','00000000-0000-0000-0000-000000000001','YOW',date_trunc('hour',NOW())-interval '30 minutes',0,1,0,'NaN'::real,-100);
 INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count) VALUES ('\xaa','00000000-0000-0000-0000-000000000001','YOW',NOW(),0,1,0) ON CONFLICT DO NOTHING;`); err != nil {
		t.Fatal(err)
	}
	raw, err := store.GetObserverActivity(ctx, id, time.Hour, 15*time.Minute, until)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := json.Marshal(raw); err != nil {
		t.Fatalf("raw non-finite sample leaked into JSON: %v", err)
	}
	if raw.Summary.RecordedPackets != 1 || raw.Summary.LastCompleteHour != 1 || raw.Summary.LatestRecordedAt == nil || raw.PayloadTypes[0].PayloadType != -1 {
		t.Fatalf("raw/duplicate: %+v", raw)
	}
	if err := store.UpsertObserverBroker(ctx, id, "one", false); err != nil {
		t.Fatal(err)
	}
	var packet pgtype.Timestamptz
	tx.QueryRow(ctx, "SELECT last_packet_at FROM observer_brokers").Scan(&packet)
	if packet.Valid {
		t.Fatal("status created a packet timestamp")
	}
	if err := store.UpsertObserverBroker(ctx, id, "one", true); err != nil {
		t.Fatal(err)
	}
	if err := store.TouchObserverBrokers(ctx, []uuid.UUID{id}, []string{"one"}, []time.Time{until.Add(time.Hour)}, []time.Time{{}}); err != nil {
		t.Fatal(err)
	}
	tx.QueryRow(ctx, "SELECT last_packet_at FROM observer_brokers").Scan(&packet)
	if !packet.Valid {
		t.Fatal("status erased packet timestamp")
	}
}
