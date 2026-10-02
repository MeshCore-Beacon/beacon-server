// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Two instances starting on an empty database must not race the baseline.
func TestRunMigrationsConcurrentStartPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	errs := make(chan error, 3)
	for range 3 {
		go func() { errs <- RunMigrations(ctx, pool) }()
	}
	for range 3 {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&n); err != nil || n != 1 {
		t.Errorf("ledger rows = %d, %v; want 1", n, err)
	}
}

// A failed ledger insert must roll back the migration it records, or a crash between
// the two leaves a schema the baseline check refuses forever.
func TestApplyAndRecordIsAtomicPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if _, err := pool.Exec(ctx, `CREATE TABLE schema_migrations (filename TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
INSERT INTO schema_migrations (filename) VALUES ('900_dup.sql')`); err != nil {
		t.Fatal(err)
	}
	if err := applyAndRecord(ctx, pool, "900_dup.sql", "CREATE TABLE atomic_probe (v INT); -- trailing comment"); err == nil {
		t.Fatal("duplicate ledger row should fail")
	}
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('atomic_probe') IS NOT NULL").Scan(&exists); err != nil || exists {
		t.Errorf("migration survived its failed ledger insert: %v %v", exists, err)
	}
	if err := applyAndRecord(ctx, pool, "901_it's.sql", "CREATE TABLE atomic_probe (v INT)"); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = '901_it''s.sql') AND to_regclass('atomic_probe') IS NOT NULL").Scan(&exists); err != nil || !exists {
		t.Errorf("migration and ledger row not both recorded: %v %v", exists, err)
	}
	// CONCURRENTLY can't share a transaction; it still applies and records.
	if err := applyAndRecord(ctx, pool, "902_idx.sql", "CREATE INDEX CONCURRENTLY atomic_probe_idx ON atomic_probe (v)"); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = '902_idx.sql') AND to_regclass('atomic_probe_idx') IS NOT NULL").Scan(&exists); err != nil || !exists {
		t.Errorf("concurrent migration not recorded: %v %v", exists, err)
	}
}

// CONCURRENTLY is illegal inside a transaction, so this uses a plain connection
// and a real table rather than the usual tx + TEMP table pattern.
func TestApplyMigrationRecoversInvalidIndexPostgres(t *testing.T) {
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

	table := fmt.Sprintf("migrate_recovery_test_%d", time.Now().UnixNano())
	index := table + "_idx"
	if _, err := conn.Exec(ctx, "CREATE TABLE "+table+" (v INT)"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+table+" CASCADE") }()

	build := "CREATE INDEX CONCURRENTLY " + index + " ON " + table + " (v)"
	if err := applyMigration(ctx, conn, build); err != nil {
		t.Fatalf("initial build: %v", err)
	}

	// A valid, already-built index (process died before recording) is a success.
	if err := applyMigration(ctx, conn, build); err != nil {
		t.Fatalf("valid existing index must be accepted: %v", err)
	}

	// Simulate an interrupted build by flipping the catalog flag.
	if _, err := conn.Exec(ctx, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass($1)", index); err != nil {
		t.Skipf("cannot mark index invalid (needs catalog write privilege): %v", err)
	}
	if err := applyMigration(ctx, conn, build); err != nil {
		t.Fatalf("invalid index must be dropped and rebuilt: %v", err)
	}
	var valid bool
	if err := conn.QueryRow(ctx, "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)", index).Scan(&valid); err != nil {
		t.Fatal(err)
	}
	if !valid {
		t.Fatal("index still invalid after recovery")
	}

	// Recovery is scoped to CONCURRENTLY migrations; a plain duplicate still errors.
	plain := "CREATE INDEX " + index + " ON " + table + " (v)"
	if err := applyMigration(ctx, conn, plain); !isDuplicateRelation(err) {
		t.Fatalf("plain duplicate index must surface 42P07, got %v", err)
	}
}
