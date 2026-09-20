// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

func applyStatsMigration(t *testing.T, ctx context.Context, tx pgx.Tx, name string) {
	t.Helper()
	migration, err := migrationFiles.ReadFile("migrations/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
}
