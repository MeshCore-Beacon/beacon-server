// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
	"time"
)

func TestScopeActivityPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = RunMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()
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
	for _, table := range []string{"nodes", "observers", "node_iatas", "node_neighbors", "observer_scopes", "transport_scopes", "node_scope_hourly", "scope_seen_paths", "scope_receiver_hourly"} {
		if _, err = tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.Exec(ctx, "SET LOCAL TIME ZONE 'Asia/Kolkata'"); err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	node, companion, a, b := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	now := time.Now().UTC()
	for i, id := range []uuid.UUID{node, companion} {
		kind := 2
		if i == 1 {
			kind = 1
		}
		if _, err = tx.Exec(ctx, "INSERT INTO nodes(id,public_key,node_type) VALUES($1,$2,$3)", id, bytes.Repeat([]byte{byte(i + 1)}, 32), kind); err != nil {
			t.Fatal(err)
		}
	}
	for i, id := range []uuid.UUID{a, b} {
		if _, err = tx.Exec(ctx, "INSERT INTO observers(id,public_key) VALUES($1,$2)", id, bytes.Repeat([]byte{byte(i + 3)}, 32)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.Exec(ctx, "INSERT INTO node_iatas(node_id,iata,last_heard) VALUES($1,'YKF',$2)", node, now); err != nil {
		t.Fatal(err)
	}
	key := sha256.Sum256([]byte("same-packet-path-identities"))
	p := ingest.ScopeObservation{NodeIDs: []uuid.UUID{node, node, companion}, ObserverID: a, IATA: "YKF", Scope: "#ykf", HeardAt: now, HashBytes: 3, PathKey: key[:], AmbiguousHops: 1}
	if err = store.RecordScopeObservation(ctx, p); err != nil {
		t.Fatal(err)
	}
	p.ObserverID = b
	if err = store.RecordScopeObservation(ctx, p); err != nil {
		t.Fatal(err)
	}
	var reports, samples int64
	if err = tx.QueryRow(ctx, "SELECT observations,unique_samples FROM node_scope_hourly WHERE node_id=$1", node).Scan(&reports, &samples); err != nil || reports != 2 || samples != 1 {
		t.Fatalf("cross-observer dedup: reports=%d samples=%d err=%v", reports, samples, err)
	}
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM node_scope_hourly WHERE node_id=$1", companion).Scan(&reports); err != nil || reports != 0 {
		t.Fatal("companion voted", reports, err)
	}
	p.IATA = "BUF"
	if err = store.RecordScopeObservation(ctx, p); err != nil {
		t.Fatal(err)
	}
	p.IATA = "YKF"
	p.Scope = "*"
	p.HeardAt = now.Add(-2 * time.Hour)
	p.UnresolvedHops = 2
	if err = store.RecordScopeObservation(ctx, p); err != nil {
		t.Fatal(err)
	}
	p.Scope = "?"
	p.HeardAt = now.Add(-8 * time.Hour)
	if err = store.RecordScopeObservation(ctx, p); err != nil {
		t.Fatal(err)
	}
	nodes := []api.NodeSummary{{ID: node, NodeType: 2}}
	if err = store.enrichNodeScopes(ctx, nodes, true); err != nil {
		t.Fatal(err)
	}
	evidence := nodes[0].ScopeEvidence
	if evidence == nil || len(evidence.Hourly) != 3 {
		t.Fatalf("hourly evidence: %+v", evidence)
	}
	for _, s := range evidence.Scopes {
		if s.Name == "*" && (*s.RepeatObservations1h != 0 || *s.RepeatObservations6h != 1 || s.RepeatObservations != 1) {
			t.Fatalf("window counts: %+v", s)
		}
	}
	activity, err := store.GetScopeActivity(ctx, nil, 24)
	if err != nil {
		t.Fatal(err)
	}
	byScope := map[string]api.ScopeActivityRow{}
	for _, s := range activity.Scopes {
		byScope[s.Name] = s
	}
	got := byScope["#ykf"]
	if got.ReceiverReports != 3 || got.UniqueSamples != 2 || got.ActiveRepeaters != 1 || got.ReceivingObservers != 2 {
		t.Fatalf("activity counts: %+v", got)
	}
	countries := map[string]int64{}
	for _, c := range activity.Countries {
		countries[c.Country] += c.Reports
	}
	if countries["CA"] != 4 || countries["US"] != 1 {
		t.Fatalf("receiving countries, never origins: %v", countries)
	}
	regional, err := store.GetScopeActivity(ctx, []string{"YKF"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(regional.Scopes) != 1 || regional.Scopes[0].ReceiverReports != 2 || regional.Scopes[0].UniqueSamples != 1 {
		t.Fatalf("receiving-area filter: %+v", regional)
	}
	if err = store.DeleteOldScopeRepeats(ctx, now.Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM scope_seen_paths WHERE hour < date_trunc('hour',now(),'UTC')-interval '1 hour'").Scan(&reports); err != nil || reports != 0 {
		t.Fatal("dedup horizon not bounded", reports, err)
	}
	if err = tx.QueryRow(ctx, "SELECT sum(unique_samples) FROM node_scope_hourly").Scan(&samples); err != nil || samples != 4 {
		t.Fatal("dedup expiry changed aggregate history", samples, err)
	}
}
