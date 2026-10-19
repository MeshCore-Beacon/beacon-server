// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	pgdata "github.com/MeshCore-Beacon/beacon-server/db/planguard"
	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Every statement of the repo corpus PREPAREs on a freshly migrated schema, in a READ ONLY transaction. A pending
// statement counts only when every table it names exists: when the server reports a missing relation (42P01) it is
// logged as skipped, and any other error fails.
func TestCorpusStatementsPrepareCleanPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "planguard_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close(context.Background())
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := db.RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	sqlc, err := planguard.LoadStatements("../../db/sqlc")
	if err != nil {
		t.Fatal(err)
	}
	c, err := planguard.LoadCorpus(pgdata.Files, "corpus.yaml", sqlc)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	pc := conn.Conn().PgConn()

	prepared, skipped := 0, 0
	names := make([]string, 0, len(c.Statements))
	for name := range c.Statements {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		st := c.Statements[name]
		text := strings.TrimRight(strings.TrimSpace(st.SQL), ";")
		_, err := pc.Exec(ctx, "BEGIN READ ONLY; PREPARE planguard_prepare_check AS\n"+text+";\nDEALLOCATE planguard_prepare_check").ReadAll()
		if _, rbErr := pc.Exec(ctx, "ROLLBACK").ReadAll(); rbErr != nil {
			t.Fatal(rbErr)
		}
		var pgErr *pgconn.PgError
		if err != nil && st.Pending != "" && errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			t.Logf("%s: pending %s: skipped (%s)", name, st.Pending, pgErr.Message)
			skipped++
			continue
		}
		if err != nil {
			t.Errorf("%s (plan %s, pending %q): PREPARE: %v", name, st.Plan, st.Pending, err)
			// A failed PREPARE leaves nothing to deallocate; make sure of it before the next statement.
			_, _ = pc.Exec(ctx, "DEALLOCATE planguard_prepare_check").ReadAll()
			continue
		}
		prepared++
	}
	var left int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_statements WHERE name = 'planguard_prepare_check'").Scan(&left); err != nil || left != 0 {
		t.Errorf("prepared statement left behind: %d, %v", left, err)
	}
	t.Logf("%d statements prepared, %d pending statements skipped", prepared, skipped)
	if prepared == 0 {
		t.Fatal("no statement prepared")
	}
}
