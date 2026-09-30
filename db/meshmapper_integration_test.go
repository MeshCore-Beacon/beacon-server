// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/meshmapper"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMeshMapperCataloguePostgres(t *testing.T) {
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
	if err := RunMigrations(ctx, pool); err != nil {
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
	for _, table := range []string{"transport_scopes", "meshmapper_scope_catalogues"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	store := &Store{q: sqlc.New(tx)}
	manual := scopestore.FromName("manual")
	manual.TransportKey[0] ^= 1
	if err := store.UpsertTransportScope(ctx, manual.Name, "Operator label", manual.TransportKey, manual.KeyFingerprint); err != nil {
		t.Fatal(err)
	}
	url := "https://yow.meshmapper.net/get_scopes.php"
	if row, err := store.GetScopeCatalogue(ctx, "YOW", url); err != nil || row != nil {
		t.Fatal(row, err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	payload := []byte(`{"generated_at":"2026-09-27T16:58:49Z","region":"YOW","zones":["YOW"],"repeaters":5,"scoped":3,"scopes":[{"name":"yow","repeaters":0,"default":0,"monitored":true,"wardriving":false}]}`)
	update := meshmapper.Cache{Payload: payload, ETag: `"v1"`, CheckedAt: now, AttemptedAt: now, NextAttempt: now.Add(time.Hour)}
	if err := store.SaveScopeCatalogue(ctx, "YOW", url, update, []scopestore.Entry{scopestore.FromName("manual"), scopestore.FromName("yow")}); err != nil {
		t.Fatal(err)
	}
	manualRows, err := store.GetTransportScopes(ctx)
	if err != nil || len(manualRows) != 1 || manualRows[0].TransportKey[0] != manual.TransportKey[0] {
		t.Fatal("manual key changed", manualRows, err)
	}
	var label string
	var imported bool
	if err := tx.QueryRow(ctx, "SELECT display_name,imported_only FROM transport_scopes WHERE name='#manual'").Scan(&label, &imported); err != nil || label != "Operator label" || imported {
		t.Fatal(label, imported, err)
	}
	row, err := store.GetScopeCatalogue(ctx, "YOW", url)
	if err != nil || row.ETag != `"v1"` || !row.CheckedAt.Equal(now) {
		t.Fatal(row, err)
	}
	scopes := scopestore.New()
	_, err = meshmapper.New(ctx, config.MeshMapperScopesConfig{Enabled: true, Sources: map[string]string{"YOW": url}}, store, scopes, manualRows)
	if err != nil || len(scopes.Entries()) != 2 {
		t.Fatal("restart did not restore imported membership", err)
	}
	// A conditional success preserves the document, advances checked time and clears a failure.
	checked := now.Add(time.Hour)
	if err := store.SaveScopeCatalogue(ctx, "YOW", url, meshmapper.Cache{ETag: `"v1"`, CheckedAt: checked, AttemptedAt: checked, NextAttempt: checked.Add(time.Hour)}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveScopeCatalogue(ctx, "YOW", url, meshmapper.Cache{AttemptedAt: checked.Add(time.Minute), NextAttempt: checked.Add(2 * time.Hour), LastError: "HTTP 429"}, nil); err != nil {
		t.Fatal(err)
	}
	row, err = store.GetScopeCatalogue(ctx, "YOW", url)
	if err != nil || !row.CheckedAt.Equal(checked) || row.ETag != `"v1"` || row.LastError != "HTTP 429" || len(row.Payload) == 0 {
		t.Fatal("failed refresh lost durable snapshot", row, err)
	}
	// Empty membership deactivates matching but retains the old scope identity for historical rows.
	empty := []byte(`{"generated_at":"2026-09-27T17:58:49Z","region":"YOW","zones":["YOW"],"repeaters":5,"scoped":0,"scopes":[]}`)
	update.Payload = empty
	update.ETag = ""
	if err := store.SaveScopeCatalogue(ctx, "YOW", url, update, nil); err != nil {
		t.Fatal(err)
	}
	_, err = meshmapper.New(ctx, config.MeshMapperScopesConfig{Enabled: true, Sources: map[string]string{"YOW": url}}, store, scopes, manualRows)
	if err != nil || len(scopes.Entries()) != 1 {
		t.Fatal("removed imported membership remained active", err)
	}
	if _, err := store.GetTransportScopeByName(ctx, "#yow"); err != nil {
		t.Fatal("deleted historical identity", err)
	}
	row, _ = store.GetScopeCatalogue(ctx, "YOW", url)
	if row.ETag != "" {
		t.Fatal("200 without ETag retained obsolete validator")
	}
	// Reconfiguring a known imported name as manual promotes it without losing its identity.
	yow := scopestore.FromName("yow")
	if err := store.UpsertTransportScope(ctx, yow.Name, "Local", yow.TransportKey, yow.KeyFingerprint); err != nil {
		t.Fatal(err)
	}
	manualRows, err = store.GetTransportScopes(ctx)
	if err != nil || len(manualRows) != 2 {
		t.Fatal("manual promotion failed", err)
	}
	// A failed cache write also rolls back the identity insertion in the same statement.
	if _, err := tx.Exec(ctx, "ALTER TABLE meshmapper_scope_catalogues ADD CHECK(last_error <> 'force-failure'); SAVEPOINT rejected"); err != nil {
		t.Fatal(err)
	}
	update.LastError = "force-failure"
	if err := store.SaveScopeCatalogue(ctx, "YOW", url, update, []scopestore.Entry{scopestore.FromName("uncommitted")}); err == nil {
		t.Fatal("expected constraint failure")
	}
	if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT rejected"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM transport_scopes WHERE name='#uncommitted'").Scan(&count); err != nil || count != 0 {
		t.Fatal("partially committed catalogue", count, err)
	}
}
