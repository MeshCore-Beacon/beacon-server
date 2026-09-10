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
