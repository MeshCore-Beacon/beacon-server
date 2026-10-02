// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
)

// Raw deletion and the rollup meet at raw_deleted_before: an hour registered but not rolled
// before its raw rows go becomes partial, while hours behind the holdback roll completely.
func TestPacketCleanupRollupHandoffPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	h0 := rollupFixture(t, ctx, pool)
	store := New(pool, 0, 0)
	old := h0.Add(-3 * time.Hour).Add(10 * time.Minute)
	if _, err := pool.Exec(ctx, `
INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header, first_heard_at, last_heard_at)
VALUES ('\xff', 2, 0, 1, '\x00', '\x00', $1::timestamptz, $1::timestamptz);
INSERT INTO packet_observations (id, packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count)
VALUES (100, '\xff', '00000000-0000-0000-0000-00000000000a', 'YVR', $1::timestamptz, 0, 1, 0)`, pgx.QueryExecModeSimpleProtocol, old); err != nil {
		t.Fatal(err)
	}
	r, ok, err := store.BeginRollup(ctx)
	if err != nil || !ok {
		t.Fatalf("BeginRollup: ok=%v err=%v", ok, err)
	}
	defer r.Close(ctx)
	if err := r.RegisterHours(ctx, h0.Add(-10*time.Hour)); err != nil {
		t.Fatal(err)
	}
	oldest, ok, err := store.OldestMissingRollupHour(ctx)
	if err != nil || !ok || !oldest.Equal(old.Truncate(time.Hour)) {
		t.Fatalf("oldest missing %s %v %v, want %s", oldest, ok, err, old.Truncate(time.Hour))
	}

	// Cleanup held behind h0 (as rawCutoff does when h0 is the oldest unrolled hour) leaves h0 intact.
	if err := store.DeleteOldPackets(ctx, h0.Add(-35*time.Minute)); err != nil {
		t.Fatal(err)
	}
	var deletedBefore time.Time
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT raw_deleted_before, (SELECT count(*) FROM packet_observations)
		FROM analytics_raw_state`).Scan(&deletedBefore, &remaining); err != nil {
		t.Fatal(err)
	}
	if !deletedBefore.Equal(old) || remaining != 10 {
		t.Fatalf("raw_deleted_before %s, %d observations left; want %s, 10", deletedBefore, remaining, old)
	}

	if err := r.MarkPartialHours(ctx); err != nil {
		t.Fatal(err)
	}
	rollAll(t, ctx, r)
	var statuses string
	if err := pool.QueryRow(ctx, `SELECT string_agg(status, ',' ORDER BY hour) FROM analytics_rollup_hours
		WHERE hour BETWEEN $1 AND $2`, old.Truncate(time.Hour), h0.Add(time.Hour)).Scan(&statuses); err != nil {
		t.Fatal(err)
	}
	if statuses != "partial,complete,complete,complete,complete" {
		t.Errorf("statuses %q, want the deleted hour partial and the rest complete", statuses)
	}
	q := rollupOracles["iata observations"]
	if got, want := rowsText(t, ctx, pool, q[0]), rowsText(t, ctx, pool, q[1]); got != want {
		t.Errorf("held-back hours lost raw rows:\nrollup:\n%s\nraw:\n%s", got, want)
	}

	if err := store.DeleteOldRollups(ctx, h0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM analytics_rollup_hours WHERE hour < $1)
		+ (SELECT count(*) FROM analytics_hourly_iata_observations WHERE hour < $1)`, h0.Add(time.Hour)).Scan(&left); err != nil || left != 0 {
		t.Errorf("rollup rows before the cutoff: %d, %v", left, err)
	}
}

// An in-flight observation insert holds KEY SHARE on its packet; cleanup skips it, and a
// second cleanup runner skips the first one's cohort instead of waiting.
func TestDeleteOldPacketsSkipsLockedPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO iata_codes (iata) VALUES ('YVR');
INSERT INTO observers (id, public_key) VALUES ('00000000-0000-0000-0000-000000000001', '\x01');
INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header, first_heard_at, last_heard_at)
VALUES ('\x01', 4, 0, 1, '\x00', '\x00', now() - INTERVAL '5 days', now() - INTERVAL '5 days')`); err != nil {
		t.Fatal(err)
	}
	params := sqlc.DeleteOldPacketsParams{Cutoff: ts(time.Now().Add(-72 * time.Hour)), BatchSize: 1000}
	q := sqlc.New(pool)

	ingest, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer ingest.Rollback(context.Background())
	if _, err := ingest.Exec(ctx, `INSERT INTO packet_observations (packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count)
VALUES ('\x01', '00000000-0000-0000-0000-000000000001', 'YVR', now() - INTERVAL '5 days', 0, 1, 0)`); err != nil {
		t.Fatal(err)
	}
	if n, err := q.DeleteOldPackets(ctx, params); err != nil || n != 0 {
		t.Fatalf("must skip in-flight reception: %d %v", n, err)
	}
	if err := ingest.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	first, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Rollback(context.Background())
	if n, err := sqlc.New(first).DeleteOldPackets(ctx, params); err != nil || n != 1 {
		t.Fatalf("first runner: %d %v", n, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if n, err := q.DeleteOldPackets(waitCtx, params); err != nil || n != 0 {
		t.Fatalf("second runner should skip the locked cohort: %d %v", n, err)
	}
	if err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM packets) + (SELECT count(*) FROM packet_observations)").Scan(&left); err != nil || left != 0 {
		t.Errorf("rows left after cleanup: %d %v", left, err)
	}
}

// Registration from an hour-aligned rollup cutoff never recreates hours cleanup just expired.
func TestRollupRetentionCyclePostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	h0 := rollupFixture(t, ctx, pool)
	store := New(pool, 0, 0)
	r, ok, err := store.BeginRollup(ctx)
	if err != nil || !ok {
		t.Fatalf("BeginRollup: ok=%v err=%v", ok, err)
	}
	defer r.Close(ctx)
	cutoff := h0.Add(30 * time.Minute) // DeleteOldRollups cutoff; registration starts at the next hour
	since := h0.Add(time.Hour)
	for range 3 {
		if err := store.DeleteOldRollups(ctx, cutoff); err != nil {
			t.Fatal(err)
		}
		if err := r.RegisterHours(ctx, since); err != nil {
			t.Fatal(err)
		}
		rollAll(t, ctx, r)
	}
	var stale int
	var first time.Time
	if err := pool.QueryRow(ctx, "SELECT count(*) FILTER (WHERE hour < $1), min(hour) FROM analytics_rollup_hours", cutoff).Scan(&stale, &first); err != nil {
		t.Fatal(err)
	}
	if stale != 0 || !first.Equal(since) {
		t.Errorf("%d expired hours re-registered, first hour %s; want 0, %s", stale, first, since)
	}
}

// Retention deletes and partial marking change what readers see, so they must move the
// revision cache keys are built from, even with no new roll.
func TestRollupCoverageMovesRevisionPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	h0 := rollupFixture(t, ctx, pool)
	store := New(pool, 0, 0)
	r, ok, err := store.BeginRollup(ctx)
	if err != nil || !ok {
		t.Fatalf("BeginRollup: ok=%v err=%v", ok, err)
	}
	defer r.Close(ctx)
	if err := r.RegisterHours(ctx, h0); err != nil {
		t.Fatal(err)
	}
	if outcome, _, err := r.RollHour(ctx, h0); err != nil || outcome != RollComplete {
		t.Fatalf("roll: %d %v", outcome, err)
	}
	step := func(name string, change bool, do func() error) {
		t.Helper()
		before := revision(t, ctx, store)
		if err := do(); err != nil {
			t.Fatal(err)
		}
		if moved := revision(t, ctx, store) != before; moved != change {
			t.Errorf("%s: revision moved=%v, want %v", name, moved, change)
		}
	}
	step("no-op cleanup", false, func() error { return store.DeleteOldRollups(ctx, h0) })
	step("partial marking", true, func() error {
		if _, err := pool.Exec(ctx, "UPDATE analytics_raw_state SET raw_deleted_before = $1", h0.Add(90*time.Minute)); err != nil {
			return err
		}
		return r.MarkPartialHours(ctx)
	})
	step("repeat partial marking", false, func() error { return r.MarkPartialHours(ctx) })
	step("retention delete", true, func() error { return store.DeleteOldRollups(ctx, h0.Add(time.Hour)) })
}
