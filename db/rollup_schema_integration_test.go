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

func TestRollupSchemaPostgres(t *testing.T) {
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
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	applyBaseline(t, ctx, tx)

	// Each failing insert runs in a savepoint so the tx stays usable.
	bad := map[string]string{
		"duplicate":   "'{YVR,YVR}'",
		"unsorted":    "'{YYZ,YVR}'",
		"empty":       "'{}'",
		"null member": "'{YVR,NULL}'",
	}
	inserts := map[string]string{
		"packet_sets": "INSERT INTO analytics_hourly_packet_sets (hour, iatas, packets) VALUES (now(), %s, 1)",
		"scope_sets":  "INSERT INTO analytics_hourly_scope_sets (hour, iatas, scope_id, packets) VALUES (now(), %s, 1, 1)",
		"advert_sets": "INSERT INTO analytics_hourly_advert_sets (hour, origin_pubkey, iatas, advert_packets, flood_packets, direct_packets) VALUES (now(), '\\x01', %s, 1, 1, 0)",
		"talker_sets": "INSERT INTO analytics_hourly_talker_sets (hour, sender_name, iatas, messages, last_sent) VALUES (now(), 'a', %s, 1, now())",
	}
	for tname, tmpl := range inserts {
		for name, lit := range bad {
			sql := fmt.Sprintf(tmpl, lit)
			if _, err := tx.Exec(ctx, "SAVEPOINT s"); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, sql); err == nil {
				t.Errorf("%s %s: insert succeeded, want CHECK violation", tname, name)
			}
			if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT s"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(tmpl, "'{YVR,YYZ}'")); err != nil {
			t.Errorf("%s canonical set: %v", tname, err)
		}
	}

	for _, tbl := range []string{"analytics_state", "analytics_raw_state"} {
		var n int
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+tbl).Scan(&n); err != nil || n != 1 {
			t.Errorf("%s rows = %d, err %v; want 1", tbl, n, err)
		}
		if _, err := tx.Exec(ctx, "SAVEPOINT s"); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO "+tbl+" (singleton) VALUES (false)"); err == nil {
			t.Errorf("%s accepted a second row", tbl)
		}
		if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT s"); err != nil {
			t.Fatal(err)
		}
	}
	var rev int64
	if err := tx.QueryRow(ctx, "SELECT revision FROM analytics_state").Scan(&rev); err != nil || rev != 0 {
		t.Errorf("revision = %d, err %v; want 0", rev, err)
	}
	var rawDeleted *time.Time
	if err := tx.QueryRow(ctx, "SELECT raw_deleted_before FROM analytics_raw_state").Scan(&rawDeleted); err != nil || rawDeleted != nil {
		t.Errorf("raw_deleted_before = %v, err %v; want NULL", rawDeleted, err)
	}

	var heard, brin, obsID, lastIATA *string
	if err := tx.QueryRow(ctx, `SELECT to_regclass('idx_observations_heard')::text, to_regclass('idx_observations_heard_brin')::text,
		to_regclass('idx_observations_observer_id')::text, to_regclass('idx_observers_last_iata')::text`).Scan(&heard, &brin, &obsID, &lastIATA); err != nil {
		t.Fatal(err)
	}
	if heard == nil || obsID == nil || lastIATA == nil {
		t.Errorf("missing index: heard=%v observer_id=%v last_iata=%v", heard, obsID, lastIATA)
	}
	if brin != nil {
		t.Errorf("idx_observations_heard_brin still exists")
	}

	var nullable string
	if err := tx.QueryRow(ctx, `SELECT is_nullable FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'channels' AND column_name = 'message_count'`).Scan(&nullable); err != nil || nullable != "NO" {
		t.Errorf("channels.message_count nullable = %q, err %v; want NO", nullable, err)
	}
	for _, c := range [][2]string{{"packets", "observation_count"}, {"observers", "last_iata"}} {
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2`, c[0], c[1]).Scan(&n); err != nil || n != 1 {
			t.Errorf("column %s.%s missing (err %v)", c[0], c[1], err)
		}
	}
	if _, err := tx.Exec(ctx, "INSERT INTO trace_tags (trace_tag, first_heard_at, last_heard_at) VALUES ('\\x01', now(), now())"); err != nil {
		t.Errorf("trace_tags: %v", err)
	}
}
