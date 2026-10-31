// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
	"time"
)

func TestNodeScopeEvidencePostgres(t *testing.T) {
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
	for _, table := range []string{"nodes", "observers", "node_neighbors", "observer_scopes", "transport_scopes", "node_scope_hourly"} {
		if _, err = tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	store := &Store{q: sqlc.New(tx)}
	node, receiver, companion := uuid.New(), uuid.New(), uuid.New()
	at := time.Now().UTC().Add(-time.Minute)
	for i, id := range []uuid.UUID{node, receiver, companion} {
		kind := 2
		if i == 2 {
			kind = 1
		}
		if _, err = tx.Exec(ctx, "INSERT INTO nodes (id,public_key,node_type) VALUES ($1,$2,$3)", id, bytes.Repeat([]byte{byte(i + 1)}, 32), kind); err != nil {
			t.Fatal(err)
		}
	}
	list := "*,ykf,on,ON"
	if err = store.UpsertNodeNeighbor(ctx, receiver, node, "YKF", nil, &list); err != nil {
		t.Fatal(err)
	}
	// A timeout/ordinary advert must not erase the configured list or advance its scope time.
	var checked time.Time
	if err = tx.QueryRow(ctx, "SELECT region_scope_checked_at FROM node_neighbors").Scan(&checked); err != nil {
		t.Fatal(err)
	}
	if err = store.UpsertNodeNeighbor(ctx, receiver, node, "YKF", nil, nil); err != nil {
		t.Fatal(err)
	}
	var preserved time.Time
	if err = tx.QueryRow(ctx, "SELECT region_scope_checked_at FROM node_neighbors").Scan(&preserved); err != nil || !checked.Equal(preserved) {
		t.Fatal("timeout rewrote scope time", err)
	}
	for _, scope := range []string{"#on", "#on", "#ykf", "*"} {
		if err = store.RecordScopeRepeats(ctx, []uuid.UUID{node, node, companion}, scope, at, 2); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.RecordScopeRepeats(ctx, []uuid.UUID{node}, "#expired", at.Add(-72*time.Hour), 1); err != nil {
		t.Fatal(err)
	}
	nodes := []api.NodeSummary{{ID: node, NodeType: 2}, {ID: companion, NodeType: 1}}
	if err = store.enrichNodeScopes(ctx, nodes); err != nil {
		t.Fatal(err)
	}
	e := nodes[0].ScopeEvidence
	if e == nil || nodes[1].ScopeEvidence != nil {
		t.Fatal("missing evidence or companion included")
	}
	found := map[string]api.NodeScope{}
	for _, s := range e.Scopes {
		found[s.Name] = s
	}
	if len(found) != 4 || found["#on"].RepeatObservations != 2 || found["*"].RepeatObservations != 1 || !found["*"].Reported || found["#ON"].RepeatObservations != 0 {
		t.Fatalf("wrong scopes: %#v", e)
	}
	if e.Scopes[0].Name != "#on" {
		t.Fatal("default/config order replaced observed ranking")
	}
	empty := ""
	if err = store.UpsertNodeNeighbor(ctx, receiver, node, "YKF", nil, &empty); err != nil {
		t.Fatal(err)
	}
	if err = store.enrichNodeScopes(ctx, nodes); err != nil {
		t.Fatal(err)
	}
	if !nodes[0].ScopeEvidence.ReportedEmpty {
		t.Fatal("explicit empty reply lost")
	}
	for _, s := range nodes[0].ScopeEvidence.Scopes {
		if s.Reported {
			t.Fatal("cleared report stayed configured")
		}
	}
	if err = store.DeleteOldScopeRepeats(ctx, time.Now().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var n int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM node_scope_hourly WHERE scope_name='#expired' OR node_id=$1", companion).Scan(&n); err != nil || n != 0 {
		t.Fatal("retention/type guard", n, err)
	}
}
