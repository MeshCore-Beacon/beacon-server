// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"os"
	"strings"
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

// noprivRole returns a NOLOGIN role without superuser, dropped on cleanup.
func noprivRole(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	const role = "beacon_test_ext_nopriv"
	if _, err := pool.Exec(ctx, "CREATE ROLE "+role+" NOLOGIN"); err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42710" {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP ROLE IF EXISTS "+role) })
	return role
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

	role := noprivRole(t, ctx, pool)

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

// A role that may create in no schema on its search_path still boots: the migration applies
// with a notice and is recorded, and the endpoint reports the extension unavailable.
func TestPgStatStatementsMigrationNoCreatableSchemaPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	body, err := migrationFiles.ReadFile("migrations/" + statementsMigration)
	if err != nil {
		t.Fatal(err)
	}
	role := noprivRole(t, ctx, pool)
	var schema string
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}

	// A dedicated connection, so the test can read the notices.
	cfg, err := pgx.ParseConfig(os.Getenv("BEACON_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["search_path"] = schema
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n.Message) }
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// The role gets no USAGE or CREATE on the only schema in its search_path. A temporary
	// ledger it may write shadows the real one, so the migration can still be recorded.
	for _, stmt := range []string{
		"DROP EXTENSION IF EXISTS pg_stat_statements",
		"CREATE TEMP TABLE schema_migrations (filename TEXT PRIMARY KEY) ON COMMIT DROP",
		"GRANT SELECT, INSERT ON pg_temp.schema_migrations TO " + role,
		"SET LOCAL ROLE " + role,
	} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if err := applyAndRecord(ctx, tx, statementsMigration, string(body)); err != nil {
		t.Fatalf("migration failed for a role with no creatable schema: %v", err)
	}
	var recorded bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)", statementsMigration).Scan(&recorded); err != nil || !recorded {
		t.Fatalf("migration not recorded: %v", err)
	}
	if n := pgStatStatementsCount(t, ctx, tx); n != 0 {
		t.Fatalf("extension created (%d rows)", n)
	}
	if len(notices) != 1 || !strings.HasPrefix(notices[0], "pg_stat_statements not created: ") {
		t.Fatalf("notices = %q; want one skip notice", notices)
	}
	if _, err := dbStatements(ctx, tx); !errors.Is(err, api.ErrStatementsUnavailable) {
		t.Fatalf("err = %v; want ErrStatementsUnavailable", err)
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
		// The probe reads this test's own schema_migrations, so its query id is unique to this
		// schema: pg_stat_statements ignores comments and keeps the first text it saw for an id.
		if err := pool.QueryRow(ctx, "-- name: ProbeStatements :one\nSELECT count(*)::int FROM schema_migrations WHERE filename <> $1", "").Scan(new(int)); err != nil {
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
