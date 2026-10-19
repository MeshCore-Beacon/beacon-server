// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const statementsMigration = "010_pg_stat_statements.sql"

func pgStatStatementsCount(t *testing.T, ctx context.Context, tx pgx.Tx) int {
	t.Helper()
	var n int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM pg_extension WHERE extname = 'pg_stat_statements'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func beginTx(t *testing.T, ctx context.Context, pool *pgxpool.Pool) pgx.Tx {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	return tx
}

// The guarded CREATE EXTENSION never fails a boot: without the privilege it records a notice, with it it creates.
func TestPgStatStatementsMigrationGuardedPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var recorded bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)", statementsMigration).Scan(&recorded); err != nil || !recorded {
		t.Fatalf("%s not in the ledger: %v", statementsMigration, err)
	}
	body, err := migrationFiles.ReadFile("migrations/" + statementsMigration)
	if err != nil {
		t.Fatal(err)
	}

	const role = "beacon_test_ext_nopriv"
	if _, err := pool.Exec(ctx, "CREATE ROLE "+role+" NOLOGIN"); err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42710" {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP ROLE IF EXISTS "+role) })

	// Without the privilege: a notice, no error, nothing created.
	tx := beginTx(t, ctx, pool)
	if _, err := tx.Exec(ctx, "DROP EXTENSION IF EXISTS pg_stat_statements"); err != nil {
		t.Fatal(err)
	}
	// Like the server's own role, it may create in its schema but not create this extension.
	var schema string
	if err := tx.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "GRANT USAGE, CREATE ON SCHEMA "+pgx.Identifier{schema}.Sanitize()+" TO "+role); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SET LOCAL ROLE "+role); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(body), pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("unprivileged run failed: %v", err)
	}
	if n := pgStatStatementsCount(t, ctx, tx); n != 0 {
		t.Fatalf("unprivileged run created the extension (%d rows)", n)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// With the privilege: created, and a rerun is a no-op.
	tx = beginTx(t, ctx, pool)
	if _, err := tx.Exec(ctx, "DROP EXTENSION IF EXISTS pg_stat_statements"); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if _, err := tx.Exec(ctx, string(body), pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("privileged run %d failed: %v", i+1, err)
		}
		if n := pgStatStatementsCount(t, ctx, tx); n != 1 {
			t.Fatalf("privileged run %d: extension rows = %d; want 1", i+1, n)
		}
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDBStatementsPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := New(pool, pool, 0, 0)
	var preloaded bool
	if err := pool.QueryRow(ctx, "SELECT current_setting('shared_preload_libraries') LIKE '%pg_stat_statements%'").Scan(&preloaded); err != nil {
		t.Fatal(err)
	}
	if !preloaded {
		if _, err := store.DBStatements(ctx); !errors.Is(err, api.ErrStatementsUnavailable) {
			t.Fatalf("without the preload: err = %v; want ErrStatementsUnavailable", err)
		}
		return
	}
	for range 3 {
		if err := pool.QueryRow(ctx, "-- name: ProbeStatements :one\nSELECT $1::int", 1).Scan(new(int)); err != nil {
			t.Fatal(err)
		}
	}
	items, err := store.DBStatements(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var probe *api.DBStatement
	for i := range items {
		if items[i].Name == "" {
			t.Fatalf("item %d has an empty name", i)
		}
		if i > 0 && items[i].ExecMs > items[i-1].ExecMs {
			t.Fatalf("items not sorted by exec_ms: %v then %v", items[i-1], items[i])
		}
		if items[i].Name == "ProbeStatements" {
			probe = &items[i]
		}
	}
	if probe == nil || probe.Calls < 3 || probe.ExecMs < 0 {
		t.Fatalf("ProbeStatements = %+v; want calls >= 3", probe)
	}
}

// With the extension absent from the database, the store reports it unavailable.
func TestDBStatementsNoExtensionPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	tx := beginTx(t, ctx, pool)
	if _, err := tx.Exec(ctx, "DROP EXTENSION IF EXISTS pg_stat_statements"); err != nil {
		t.Fatal(err)
	}
	_, err := dbStatements(ctx, tx)
	if !errors.Is(err, api.ErrStatementsUnavailable) {
		t.Fatalf("err = %v; want ErrStatementsUnavailable", err)
	}
}
