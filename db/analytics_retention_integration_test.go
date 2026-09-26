// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"os"
	"testing"
	"time"
)

var retainedViews = []string{"mv_hourly_iata_stats", "mv_payload_breakdown_by_iata", "mv_top_observers_by_iata", "mv_top_talkers_by_iata", "mv_top_advertisers_by_iata", "mv_observer_activity_hourly", "mv_signal_stats_hourly", "mv_path_stats_hourly"}

func analyticsSnapshot(t *testing.T, ctx context.Context, tx pgx.Tx) map[string]string {
	t.Helper()
	result := map[string]string{}
	for _, view := range retainedViews {
		if _, err := tx.Exec(ctx, "REFRESH MATERIALIZED VIEW "+view); err != nil {
			t.Fatal(err)
		}
		var rows string
		if err := tx.QueryRow(ctx, "SELECT COALESCE(jsonb_agg(r ORDER BY r::text),'[]')::text FROM (SELECT to_jsonb(v) r FROM "+view+" v) s").Scan(&rows); err != nil {
			t.Fatal(err)
		}
		result[view] = rows
	}
	return result
}

func TestAnalyticsRetentionConcurrentPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	analyticsTables(t, ctx, tx)
	applyStatsMigration(t, ctx, tx, "039_analytics_retention.sql")
	if _, err := tx.Exec(ctx, `INSERT INTO packets (packet_hash,last_heard_at) VALUES ('\x01',NOW()-interval '5 days'); INSERT INTO observers(id) VALUES ('00000000-0000-0000-0000-000000000001')`); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := tx.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	conn := tx.Conn()
	quoted := pgx.Identifier{schema}.Sanitize()
	t.Cleanup(func() {
		if _, err := conn.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	if _, err := conn.Exec(ctx, "SET search_path TO "+quoted); err != nil {
		t.Fatal(err)
	}
	writer, err := pgx.Connect(ctx, os.Getenv("BEACON_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(context.Background())
	if _, err = writer.Exec(ctx, "SET search_path TO "+quoted); err != nil {
		t.Fatal(err)
	}
	ingest, err := writer.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer ingest.Rollback(context.Background())
	// An in-flight FK insert holds KEY SHARE on the packet until it commits.
	if _, err = ingest.Exec(ctx, `INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at) VALUES ('\x01','00000000-0000-0000-0000-000000000001','YVR',NOW()-interval '5 days')`); err != nil {
		t.Fatal(err)
	}
	q := sqlc.New(conn)
	params := sqlc.DeleteOldPacketsParams{Cutoff: pgtype.Timestamptz{Time: time.Now().Add(-72 * time.Hour), Valid: true}, BatchSize: 1000}
	if n, err := q.DeleteOldPackets(ctx, params); err != nil || n != 0 {
		t.Fatalf("must skip in-flight reception: %d %v", n, err)
	}
	if err := ingest.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := q.DeleteOldPackets(ctx, params); err != nil || n != 1 {
		t.Fatalf("retry after ingestion: %d %v", n, err)
	}
	var count int
	if err := conn.QueryRow(ctx, "SELECT observation_count FROM analytics_hourly_iata_stats").Scan(&count); err != nil || count != 1 {
		t.Fatalf("committed reception not archived: %d %v", count, err)
	}
}

// Minimal real tables keep this regression runnable against an empty CI database.
func analyticsTables(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	isolateStatsSchema(t, ctx, tx)
	_, err := tx.Exec(ctx, `
CREATE TABLE packets (packet_hash bytea PRIMARY KEY, payload_type smallint, payload_version smallint,
 route_type smallint, raw_payload bytea, raw_header bytea, origin_pubkey bytea,
 first_heard_at timestamptz, last_heard_at timestamptz);
CREATE INDEX ON packets(last_heard_at);
CREATE TABLE packet_observations (id bigserial PRIMARY KEY, packet_hash bytea REFERENCES packets ON DELETE CASCADE,
 observer_id uuid NOT NULL, iata char(3) NOT NULL, heard_at timestamptz NOT NULL,
 path_length_byte smallint, hash_size smallint, hop_count smallint, path_bytes bytea,
 snr real, rssi smallint, airtime_ms real, payload_type smallint, UNIQUE(packet_hash,observer_id));
CREATE TABLE observers (id uuid PRIMARY KEY, public_key bytea, display_name text, observer_type text);
CREATE TABLE nodes (id uuid PRIMARY KEY, public_key bytea UNIQUE, node_type smallint, name text);
CREATE TABLE channel_messages (id bigserial PRIMARY KEY, channel_id integer, packet_hash bytea UNIQUE REFERENCES packets ON DELETE CASCADE,
 sender_name text, content text, sent_at timestamptz);
`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAnalyticsRetentionPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	analyticsTables(t, ctx, tx)
	_, err := tx.Exec(ctx, `
 INSERT INTO observers (id,public_key,display_name,observer_type) SELECT ('00000000-0000-0000-0000-00000000000'||i)::uuid,int4send(i),CASE WHEN i=1 THEN NULL ELSE 'Observer '||i END,CASE WHEN i=1 THEN NULL ELSE 'test' END FROM generate_series(1,3) i;
 INSERT INTO nodes (id,public_key,node_type,name) VALUES ('00000000-0000-0000-0000-000000000002','\x02',2,'Repeater');
 INSERT INTO packets (packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,origin_pubkey,first_heard_at,last_heard_at)
 SELECT int4send(i),4,0,CASE WHEN i%2=0 THEN 1 ELSE 2 END,'\x00','\x00','\x02',date_trunc('hour',NOW())-interval '5 days',
 CASE WHEN i=4 THEN NOW() ELSE date_trunc('hour',NOW())-interval '5 days' END FROM generate_series(1,4) i;
 INSERT INTO packet_observations (packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,snr,rssi,airtime_ms,payload_type)
 SELECT packet_hash,('00000000-0000-0000-0000-00000000000'||n)::uuid,iata,first_heard_at+make_interval(secs=>n),0,1,0,
 CASE WHEN n=1 THEN 0 ELSE NULL END,CASE WHEN n=1 THEN -100 ELSE NULL END,1.5,4
 FROM packets CROSS JOIN (VALUES ('YVR',1),('YVR',2),('YYZ',3)) v(iata,n);
 INSERT INTO channel_messages (channel_id,packet_hash,sender_name,content,sent_at)
 SELECT 1,packet_hash,'Sender','body that must expire',first_heard_at FROM packets;
 CREATE MATERIALIZED VIEW mv_hourly_iata_stats AS SELECT iata,date_trunc('hour',heard_at) AS hour,count(*) AS observation_count,count(DISTINCT packet_hash) AS unique_packets,count(DISTINCT observer_id) AS active_observers FROM packet_observations GROUP BY 1,2;
 `)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range []string{"016_mv_payload_breakdown.sql", "017_mv_top_observers.sql", "018_mv_top_talkers.sql", "019_mv_top_advertisers.sql", "021_mv_top_advertisers_route_type.sql", "032_mv_observer_activity.sql", "035_mv_signal_stats.sql", "036_mv_path_stats.sql"} {
		applyStatsMigration(t, ctx, tx, migration)
	}
	applyStatsMigration(t, ctx, tx, "039_analytics_retention.sql")
	before := analyticsSnapshot(t, ctx, tx)
	q := sqlc.New(tx)
	cutoff := time.Now().Add(-72 * time.Hour)
	// Failure in one archive must roll back every archive write and the raw deletion.
	if _, err := tx.Exec(ctx, `SAVEPOINT archive_failure; ALTER TABLE analytics_signal_stats_hourly ADD CONSTRAINT fail_archive CHECK(receptions < 0) NOT VALID`); err != nil {
		t.Fatal(err)
	}
	if _, err := q.DeleteOldPackets(ctx, sqlc.DeleteOldPacketsParams{Cutoff: pgtype.Timestamptz{Time: cutoff, Valid: true}, BatchSize: 1}); err == nil {
		t.Fatal("expected archive failure")
	}
	if _, err := tx.Exec(ctx, "ROLLBACK TO archive_failure"); err != nil {
		t.Fatal(err)
	}
	if countRows(t, ctx, tx, "SELECT count(*) FROM packets") != 4 {
		t.Fatal("archive failure deleted raw packets")
	}
	for i := 0; i < 3; i++ {
		n, err := q.DeleteOldPackets(ctx, sqlc.DeleteOldPacketsParams{Cutoff: pgtype.Timestamptz{Time: cutoff, Valid: true}, BatchSize: 1})
		if err != nil || n != 1 {
			t.Fatalf("delete batch: %d %v", n, err)
		}
	}
	if countRows(t, ctx, tx, "SELECT count(*) FROM packets") != 1 || countRows(t, ctx, tx, "SELECT count(*) FROM channel_messages") != 1 {
		t.Fatal("raw packets/messages did not expire")
	}
	after := analyticsSnapshot(t, ctx, tx)
	for _, view := range retainedViews {
		if after[view] != before[view] {
			t.Errorf("%s lost or double-counted history after raw expiry: before=%s after=%s", view, before[view], after[view])
		}
	}
	// Retry and migration journal retry preserve the exact summaries.
	if n, err := q.DeleteOldPackets(ctx, sqlc.DeleteOldPacketsParams{Cutoff: pgtype.Timestamptz{Time: cutoff, Valid: true}, BatchSize: 1000}); err != nil || n != 0 {
		t.Fatalf("retry: %d %v", n, err)
	}
	applyStatsMigration(t, ctx, tx, "039_analytics_retention.sql")
	for view, rows := range analyticsSnapshot(t, ctx, tx) {
		if rows != before[view] {
			t.Errorf("retry changed %s", view)
		}
	}
	// Later receptions on a still-live packet must join its old bucket before expiry.
	if _, err := tx.Exec(ctx, `INSERT INTO observers (id) VALUES ('00000000-0000-0000-0000-000000000004'); INSERT INTO packet_observations (packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,snr,rssi,payload_type)
 SELECT packet_hash,'00000000-0000-0000-0000-000000000004','YVR',first_heard_at,0,1,0,'NaN',-80,NULL FROM packets;
 UPDATE packets SET last_heard_at=first_heard_at`); err != nil {
		t.Fatal(err)
	}
	late := analyticsSnapshot(t, ctx, tx)
	if err := (&Store{q: q}).DeleteOldPackets(ctx, cutoff); err != nil {
		t.Fatal(err)
	}
	for view, rows := range analyticsSnapshot(t, ctx, tx) {
		if rows != late[view] {
			t.Errorf("late reception changed %s: %s != %s", view, rows, late[view])
		}
	}
	// Archived labels follow a current rename; no entity FK may erase retained history.
	if _, err := tx.Exec(ctx, `UPDATE nodes SET name='Renamed'; UPDATE observers SET display_name=NULL,observer_type=NULL;`); err != nil {
		t.Fatal(err)
	}
	analyticsSnapshot(t, ctx, tx)
	if countRows(t, ctx, tx, "SELECT count(*) FROM mv_top_advertisers_by_iata WHERE name='Renamed'") != 2 {
		t.Fatal("archive name did not follow rename")
	}
	if _, err := q.GetStatsTopObservers(ctx, sqlc.GetStatsTopObserversParams{Column1: pgtype.Interval{Microseconds: int64(30 * 24 * time.Hour / time.Microsecond), Valid: true}, Limit: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM nodes; DELETE FROM observers"); err != nil {
		t.Fatal(err)
	}
	if countRows(t, ctx, tx, "SELECT count(*) FROM analytics_top_advertisers_by_iata") != 2 {
		t.Fatal("entity deletion erased archive")
	}
	// Independent expiry runs even when there are no packets left to delete.
	for _, view := range retainedViews {
		table := "analytics_" + view[3:]
		at := "bucket"
		if view == "mv_hourly_iata_stats" || view == "mv_signal_stats_hourly" || view == "mv_path_stats_hourly" {
			at = "hour"
		}
		if _, err := tx.Exec(ctx, "UPDATE "+table+" SET "+at+"="+at+"-interval '31 days'"); err != nil {
			t.Fatal(err)
		}
	}
	if err := (&Store{q: q}).DeleteOldPackets(ctx, cutoff); err != nil {
		t.Fatal(err)
	}
	for _, view := range retainedViews {
		if countRows(t, ctx, tx, "SELECT count(*) FROM analytics_"+view[3:]) != 0 {
			t.Errorf("%s archive did not expire", view)
		}
	}
	// More than one populated cohort, with shared hourly keys across the boundary.
	if _, err := tx.Exec(ctx, `
INSERT INTO observers (id) VALUES ('00000000-0000-0000-0000-000000000001');
INSERT INTO nodes (id,public_key,node_type) VALUES ('00000000-0000-0000-0000-000000000002','\x02',2);
INSERT INTO packets (packet_hash,payload_type,route_type,origin_pubkey,first_heard_at,last_heard_at,raw_payload)
SELECT int4send(i),4,1,'\x02',date_trunc('hour',NOW())-interval '5 days',date_trunc('hour',NOW())-interval '5 days',decode(repeat('ab',1024),'hex') FROM generate_series(10000,11000) i;
INSERT INTO packet_observations (packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type,snr,rssi)
SELECT packet_hash,'00000000-0000-0000-0000-000000000001','YVR',first_heard_at,0,1,0,4,0,-100 FROM packets;
INSERT INTO channel_messages (packet_hash,sender_name,sent_at) SELECT packet_hash,'Sender',first_heard_at FROM packets;
`); err != nil {
		t.Fatal(err)
	}
	populated := analyticsSnapshot(t, ctx, tx)
	started := time.Now()
	if err := (&Store{q: q}).DeleteOldPackets(ctx, cutoff); err != nil {
		t.Fatal(err)
	}
	t.Logf("1001 packets with bodies and observations archived/deleted in %s", time.Since(started))
	for view, rows := range analyticsSnapshot(t, ctx, tx) {
		if rows != populated[view] {
			t.Errorf("batch boundary changed %s", view)
		}
	}
	var archiveRows int
	for _, view := range retainedViews {
		archiveRows += countRows(t, ctx, tx, "SELECT count(*) FROM analytics_"+view[3:])
	}
	if archiveRows > 12 {
		t.Fatalf("expected compact rollups, got %d archive rows for 1001 packets", archiveRows)
	}
}
