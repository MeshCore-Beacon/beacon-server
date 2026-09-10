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

func TestChannelIndexMigrationsPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	exec := func(query string) {
		t.Helper()
		if _, err := conn.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	schema := pgx.Identifier{fmt.Sprintf("channel_index_test_%d", time.Now().UnixNano())}.Sanitize()
	exec("CREATE SCHEMA " + schema)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := conn.Exec(cleanup, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	exec("SET search_path TO " + schema)
	exec(`CREATE TABLE channels (id bigint PRIMARY KEY, last_seen timestamptz NOT NULL);
CREATE INDEX idx_channels_last_seen ON channels(last_seen DESC);
INSERT INTO channels VALUES (1, '2026-09-09 12:00:00+00');`)
	migrate := func(name string) {
		t.Helper()
		query, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		exec(string(query)) // Concurrent DDL must remain outside an explicit transaction.
	}
	checkReplacement := func() {
		t.Helper()
		var valid bool
		if err := conn.QueryRow(ctx, `SELECT indisvalid AND indisready AND indnkeyatts=2
FROM pg_index WHERE indexrelid='idx_channels_last_seen_id'::regclass`).Scan(&valid); err != nil || !valid {
			t.Fatalf("replacement index valid=%v, err=%v", valid, err)
		}
	}
	migrate("028_channels_last_seen_id.sql")
	checkReplacement()
	for range 2 { // Retry after the drop succeeds but before its migration record is saved.
		migrate("029_drop_channels_last_seen.sql")
		var preserved bool
		if err := conn.QueryRow(ctx, `SELECT to_regclass('idx_channels_last_seen') IS NULL
AND (SELECT count(*) FROM channels)=1`).Scan(&preserved); err != nil || !preserved {
			t.Fatalf("old index removed and row preserved=%v, err=%v", preserved, err)
		}
		checkReplacement()
	}
}
