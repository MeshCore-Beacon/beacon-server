// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"
	"os"
	"strings"
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
	if want := len(embeddedMigrations(t)); pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&n) != nil || n != want {
		t.Errorf("ledger rows = %d; want %d", n, want)
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

const drop004 = "004_drop_known_routes_iata_last_seen.sql"

// Every index the migrations leave must be usable, and 004 must also apply where an operator already dropped it by hand.
func TestRunMigrationsIndexesValidPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var total int
	var bad []string
	if err := pool.QueryRow(ctx, `SELECT count(*), coalesce(array_agg(c.relname::text) FILTER (WHERE NOT (i.indisvalid AND i.indisready)), '{}')
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relnamespace = current_schema()::regnamespace`).Scan(&total, &bad); err != nil {
		t.Fatal(err)
	}
	if total == 0 || len(bad) != 0 {
		t.Fatalf("%d indexes, not valid and ready: %v", total, bad)
	}
	var gone bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('idx_known_routes_iata_last_seen') IS NULL").Scan(&gone); err != nil || !gone {
		t.Fatalf("idx_known_routes_iata_last_seen survived migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM schema_migrations WHERE filename = '"+drop004+"'"); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("004 after a manual drop: %v", err)
	}
	var recorded bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)", drop004).Scan(&recorded); err != nil || !recorded {
		t.Fatalf("004 not recorded after a manual drop: %v", err)
	}
}

const hopIndex006 = "006_known_routes_hop_iata.sql"

// An operator may pre-build the hop index; the migration must record it, not fail or rebuild it.
func TestRunMigrationsHopIndexPrebuiltPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var def string
	if err := pool.QueryRow(ctx, "SELECT pg_get_indexdef('idx_known_routes_hop_iata'::regclass)").Scan(&def); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(def, "USING btree (hop_count, iata)") {
		t.Fatalf("hop index = %q", def)
	}
	var stateTable bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('background_state') IS NOT NULL").Scan(&stateTable); err != nil || !stateTable {
		t.Fatalf("background_state missing: %v", err)
	}
	// An operator pre-built the index by hand under the migration's exact name, then the binary boots.
	if _, err := pool.Exec(ctx, "DELETE FROM schema_migrations WHERE filename = '"+hopIndex006+"'"); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("006 over a pre-built index: %v", err)
	}
	var recorded bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)", hopIndex006).Scan(&recorded); err != nil || !recorded {
		t.Fatalf("006 not recorded: %v", err)
	}
}

// The migrations leave known_routes with exactly the indexes some statement reads, all usable.
func TestKnownRoutesIndexSetPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var got, bad []string
	if err := pool.QueryRow(ctx, `SELECT coalesce(array_agg(indexrelid::regclass::text ORDER BY indexrelid::regclass::text), '{}'),
       coalesce(array_agg(indexrelid::regclass::text) FILTER (WHERE NOT (indisvalid AND indisready)), '{}')
FROM pg_index WHERE indrelid = 'known_routes'::regclass`).Scan(&got, &bad); err != nil {
		t.Fatal(err)
	}
	want := []string{"idx_known_routes_hop_iata", "idx_known_routes_iata_keyset", "idx_known_routes_keyset", "idx_known_routes_node_ids", "known_routes_pkey"}
	if strings.Join(got, ",") != strings.Join(want, ",") || len(bad) != 0 {
		t.Fatalf("known_routes indexes = %v, want %v; not valid and ready: %v", got, want, bad)
	}
}

// The operator pre-builds the four packet indexes; the migrations record them, not fail or rebuild them.
func TestRunMigrationsPacketsIndexesPrebuiltPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	oids := map[string]uint32{}
	for _, idx := range packetsEndStateIndexes {
		var oid uint32
		if err := pool.QueryRow(ctx, "SELECT $1::regclass::oid", idx).Scan(&oid); err != nil {
			t.Fatalf("%s: %v", idx, err)
		}
		oids[idx] = oid
	}
	files := []string{
		"029_packets_last_heard_hash.sql",
		"030_packets_type_last_heard.sql",
		"031_packets_scope_last_heard.sql",
		"032_packets_heard_inverted.sql",
	}
	if _, err := pool.Exec(ctx, "DELETE FROM schema_migrations WHERE filename = ANY($1)", files); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("029-032 over pre-built indexes: %v", err)
	}
	checkRecorded(t, ctx, pool, files...)
	for idx, before := range oids {
		var after uint32
		if err := pool.QueryRow(ctx, "SELECT $1::regclass::oid", idx).Scan(&after); err != nil {
			t.Fatalf("%s: %v", idx, err)
		}
		if after != before {
			t.Errorf("%s rebuilt: oid %d -> %d", idx, before, after)
		}
	}
}
