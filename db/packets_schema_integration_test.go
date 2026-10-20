// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// packetsEndStateMigrations are the packet type domains, row-cursor indexes and the inverted-heard statistics.
var packetsEndStateMigrations = []string{
	"025_packets_payload_type_domain.sql",
	"026_validate_packets_payload_type_domain.sql",
	"027_packets_route_type_domain.sql",
	"028_validate_packets_route_type_domain.sql",
	"029_packets_last_heard_hash.sql",
	"030_packets_type_last_heard.sql",
	"031_packets_scope_last_heard.sql",
	"032_packets_heard_inverted.sql",
	"033_packets_heard_inverted_stat.sql",
}

// packetsEndStateIndexes are the indexes packetsEndStateMigrations build.
var packetsEndStateIndexes = []string{
	"idx_packets_last_heard_hash",
	"idx_packets_type_last_heard",
	"idx_packets_scope_last_heard",
	"idx_packets_heard_inverted",
}

// applyPacketsEndState applies packetsEndStateMigrations inside tx.
func applyPacketsEndState(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	applyMigrationsTx(t, ctx, tx, packetsEndStateMigrations...)
}

// The domain checks are validated and the four packet indexes and the statistics object exist and are usable.
func TestPacketsDomainChecksPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{
		"packets_payload_type_domain": "CHECK (((payload_type >= 0) AND (payload_type <= 15)))",
		"packets_route_type_domain":   "CHECK (((route_type >= 0) AND (route_type <= 3)))",
	}
	rows, err := pool.Query(ctx, `SELECT conname::text, convalidated, pg_get_constraintdef(oid)
FROM pg_constraint WHERE conrelid = 'packets'::regclass AND contype = 'c'`)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for rows.Next() {
		var name, def string
		var validated bool
		if err := rows.Scan(&name, &validated, &def); err != nil {
			t.Fatal(err)
		}
		if !validated {
			t.Errorf("%s not validated", name)
		}
		got[name] = def
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("packets checks = %v, want %v", got, want)
	}
	for name, def := range want {
		if got[name] != def {
			t.Errorf("%s = %q, want %q", name, got[name], def)
		}
	}

	for _, idx := range packetsEndStateIndexes {
		var usable bool
		if err := pool.QueryRow(ctx, `SELECT i.indisvalid AND i.indisready FROM pg_index i
WHERE i.indexrelid = to_regclass($1)`, idx).Scan(&usable); err != nil {
			t.Fatalf("%s: %v", idx, err)
		}
		if !usable {
			t.Errorf("%s not valid and ready", idx)
		}
	}
	var def string
	if err := pool.QueryRow(ctx, "SELECT pg_get_indexdef('idx_packets_heard_inverted'::regclass)").Scan(&def); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(def, "WHERE ((first_heard_at > last_heard_at) IS TRUE)") {
		t.Errorf("idx_packets_heard_inverted = %q", def)
	}

	var stats int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_statistic_ext WHERE stxname = 'packets_heard_inverted_stat' AND stxrelid = 'packets'::regclass").Scan(&stats); err != nil {
		t.Fatal(err)
	}
	if stats != 1 {
		t.Errorf("packets_heard_inverted_stat rows = %d, want 1", stats)
	}

	for _, tc := range []struct {
		name                   string
		payloadType, routeType int
	}{
		{"payload_type 16", 16, 0},
		{"route_type 4", 0, 4},
	} {
		_, err := pool.Exec(ctx, `INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header, first_heard_at, last_heard_at)
VALUES ('\x01', $1, 0, $2, '\x', '\x', now(), now())`, tc.payloadType, tc.routeType)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Errorf("%s: err = %v, want SQLSTATE 23514", tc.name, err)
		}
	}
}
