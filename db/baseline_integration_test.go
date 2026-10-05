// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Views cannot reference temporary tables. Keep fixtures and real migration DDL
// in a unique schema inside the caller's rolled-back transaction instead.
func isolateStatsSchema(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	schema := pgx.Identifier{"stats_test_" + uuid.NewString()}.Sanitize()
	if _, err := tx.Exec(ctx, "CREATE SCHEMA "+schema+"; SET LOCAL search_path TO "+schema); err != nil {
		t.Fatal(err)
	}
}

// applyBaseline builds the real schema in an isolated schema, plus the IATAs fixtures use.
func applyBaseline(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	isolateStatsSchema(t, ctx, tx)
	ddl, err := migrationFiles.ReadFile("migrations/" + baselineMigration)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(ddl)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO iata_codes (iata) VALUES ('YVR'),('YYJ'),('YYZ'),('YOW')"); err != nil {
		t.Fatal(err)
	}
}

// schemaPool returns a pool whose connections all use a fresh, dropped-on-cleanup schema.
func schemaPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	setup, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "migrate_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := setup.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := setup.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
		setup.Close(context.Background())
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool
}

func TestRunMigrationsBaselinePostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	for range 2 {
		if err := RunMigrations(ctx, pool); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := pool.Query(ctx, "SELECT filename FROM schema_migrations ORDER BY filename")
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(ledger) != 1 || ledger[0] != baselineMigration {
		t.Fatalf("ledger %v, %v", ledger, err)
	}
	// Stats refresh uses CONCURRENTLY, which needs populated views.
	if _, err := pool.Exec(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_radio_presets"); err != nil {
		t.Fatal(err)
	}
}

func TestRunMigrationsRefusesPreBaselinePostgres(t *testing.T) {
	for name, setup := range map[string]string{
		"1.x ledger":     "CREATE TABLE schema_migrations (filename text PRIMARY KEY); INSERT INTO schema_migrations VALUES ('001_initial_schema.sql')",
		"pre-ledger 1.x": "CREATE TABLE packets (packet_hash bytea PRIMARY KEY)",
	} {
		t.Run(name, func(t *testing.T) {
			ctx, pool := schemaPool(t)
			if _, err := pool.Exec(ctx, setup); err != nil {
				t.Fatal(err)
			}
			if err := RunMigrations(ctx, pool); !errors.Is(err, errPreBaseline) {
				t.Fatalf("got %v, want errPreBaseline", err)
			}
		})
	}
}
