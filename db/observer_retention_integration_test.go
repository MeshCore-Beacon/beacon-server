// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/presence"
	"github.com/jackc/pgx/v5"
)

func TestObserverRetentionPostgres(t *testing.T) {
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
	for _, table := range []string{"observers", "packet_observations", "observer_telemetry", "observer_owners", "observer_brokers", "observer_locations", "observer_scopes"} {
		exec("CREATE TEMP TABLE " + table + " (LIKE public." + table + " INCLUDING ALL) ON COMMIT DROP")
		if table != "observers" {
			fk := "ALTER TABLE pg_temp." + table + " ADD FOREIGN KEY (observer_id) REFERENCES pg_temp.observers(id)"
			if table != "packet_observations" {
				fk += " ON DELETE CASCADE"
			}
			exec(fk)
		}
	}
	store := &Store{q: sqlc.New(tx)}
	cutoff := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	exec(`INSERT INTO observers(public_key,display_name,last_seen,last_status_at)
SELECT decode(lpad(to_hex(i),64,'0'),'hex'),label,$1::timestamptz+seen*interval '1 second',
 $1::timestamptz+status*interval '1 second'
FROM (VALUES (1,'expired',-1,NULL),(2,'boundary',0,NULL),(3,'recent',1,NULL),
 (4,'recent status',-1,1),(5,'status boundary',-1,0),
 (6,'packet history',-1,NULL),(7,'telemetry',-1,NULL),(8,'owned',-1,NULL)) v(i,label,seen,status)`, cutoff)
	exec(`INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
SELECT 1,'\x01',id,'YYZ',last_seen,0,1,0 FROM observers WHERE display_name='packet history';
INSERT INTO observer_telemetry(id,observer_id,reported_at)
SELECT 1,id,last_seen FROM observers WHERE display_name='telemetry';
INSERT INTO observer_owners(observer_id,notes)
SELECT id,'fixture ownership without a node link' FROM observers WHERE display_name='owned';
INSERT INTO observer_brokers(observer_id,broker_name)
SELECT id,'fixture' FROM observers WHERE display_name='expired';
INSERT INTO observer_locations(observer_id,reported_at)
SELECT id,last_seen FROM observers WHERE display_name='expired';
INSERT INTO observer_scopes(observer_id,scope_id)
SELECT id,1 FROM observers WHERE display_name='expired';`)
	ids, err := store.DeleteOldObservers(ctx, cutoff)
	if err != nil || len(ids) != 1 {
		t.Fatalf("expected one unreferenced expired observer deleted, got %d: %v", len(ids), err)
	}
	var remaining string
	if err := tx.QueryRow(ctx, "SELECT string_agg(display_name,',' ORDER BY display_name) FROM observers").Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != "boundary,owned,packet history,recent,recent status,status boundary,telemetry" {
		t.Fatalf("incorrect retained observers: %s", remaining)
	}
	var history, metadata int
	if err := tx.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM packet_observations)+(SELECT count(*) FROM observer_telemetry)+(SELECT count(*) FROM observer_owners),
 (SELECT count(*) FROM observer_brokers)+(SELECT count(*) FROM observer_locations)+(SELECT count(*) FROM observer_scopes)`).Scan(&history, &metadata); err != nil {
		t.Fatal(err)
	}
	if history != 3 || metadata != 0 {
		t.Fatalf("history=%d, cascading metadata=%d; want 3, 0", history, metadata)
	}
	if ids, err := store.DeleteOldObservers(ctx, cutoff); err != nil || len(ids) != 0 {
		t.Fatalf("repeated cleanup changed protected rows: %v, %v", ids, err)
	}

	// All relations below are temporary; no persistent fixtures or sequences are used.
	exec("TRUNCATE pg_temp.observers CASCADE")
	coalescer := presence.New(store, time.Second, time.Second)
	pubkey := bytes.Repeat([]byte{42}, 32)
	oldID, _, err := coalescer.UpsertObserver(ctx, pubkey)
	if err != nil {
		t.Fatal(err)
	}
	if err := coalescer.UpsertObserverBroker(ctx, oldID, "fixture"); err != nil {
		t.Fatal(err)
	}
	// A future cutoff simulates this cached observer having aged past retention.
	ids, err = coalescer.DeleteOldObservers(ctx, time.Now().Add(time.Hour))
	if err != nil || len(ids) != 1 || ids[0] != oldID {
		t.Fatalf("cached expired observer was not deleted: %v, %v", ids, err)
	}
	newID, _, err := coalescer.UpsertObserver(ctx, pubkey)
	if err != nil || newID == oldID {
		t.Fatalf("returning observer reused a deleted ID: %v", err)
	}
	if err := coalescer.UpsertObserverBroker(ctx, newID, "fixture"); err != nil {
		t.Fatal(err)
	}
	var brokers int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM observer_brokers WHERE observer_id=$1", newID).Scan(&brokers); err != nil || brokers != 1 {
		t.Fatalf("returning observer's broker was not recreated: count=%d, err=%v", brokers, err)
	}
	// The persisted row looks stale while fresh activity is still coalesced.
	cutoff = time.Now().Add(-24 * time.Hour)
	exec("UPDATE observers SET last_seen=$1 WHERE id=$2", cutoff.Add(-time.Hour), newID)
	if _, _, err := coalescer.UpsertObserver(ctx, pubkey); err != nil {
		t.Fatal(err)
	}
	if ids, err := coalescer.DeleteOldObservers(ctx, cutoff); err != nil || len(ids) != 0 {
		t.Fatalf("pending presence did not protect a returning observer: %v, %v", ids, err)
	}
	var fresh bool
	if err := tx.QueryRow(ctx, "SELECT last_seen >= $1 AND observation_count=1 FROM observers WHERE id=$2", cutoff, newID).Scan(&fresh); err != nil || !fresh {
		t.Fatalf("pending presence was not persisted before cleanup: %v", err)
	}

	exec("TRUNCATE pg_temp.observers CASCADE")
	exec(`INSERT INTO observers(public_key,last_seen)
SELECT decode(lpad(to_hex(i),64,'0'),'hex'),$1::timestamptz-interval '1 second'
FROM generate_series(1,1005) i`, cutoff)
	for _, want := range []int{1000, 5, 0} {
		ids, err := store.DeleteOldObservers(ctx, cutoff)
		if err != nil || len(ids) != want {
			t.Fatalf("bounded cleanup removed %d observers, want %d: %v", len(ids), want, err)
		}
		t.Logf("bounded cleanup deleted %d observers", len(ids))
	}
}
