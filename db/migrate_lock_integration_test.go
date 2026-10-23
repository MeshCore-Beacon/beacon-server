// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func shortLockWaits(t *testing.T, timeout time.Duration, backoff ...time.Duration) {
	t.Helper()
	oldT, oldB, oldP := migrationLockTimeout, migrationLockBackoff, blockerPollInterval
	migrationLockTimeout, migrationLockBackoff, blockerPollInterval = timeout, backoff, 50*time.Millisecond
	t.Cleanup(func() { migrationLockTimeout, migrationLockBackoff, blockerPollInterval = oldT, oldB, oldP })
}

// rearm004 puts the schema back to "004 pending, index present".
func rearm004(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(t.Context(), "CREATE INDEX idx_known_routes_iata_last_seen ON known_routes (iata, last_seen DESC); DELETE FROM schema_migrations WHERE filename = '"+drop004+"'"); err != nil {
		t.Fatal(err)
	}
}

// lockKnownRoutes holds, in an open transaction, the lock DROP/CREATE INDEX CONCURRENTLY wait on.
func lockKnownRoutes(t *testing.T, pool *pgxpool.Pool) (pgx.Tx, int32) {
	t.Helper()
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) }) // before schemaPool's pool.Close
	if _, err := tx.Exec(t.Context(), "LOCK TABLE known_routes IN SHARE UPDATE EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	var pid int32
	if err := tx.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return tx, pid
}

func TestRunMigrationsLockTimeoutPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	rearm004(t, pool)
	shortLockWaits(t, 300*time.Millisecond, 50*time.Millisecond)
	blocker, blockerPID := lockKnownRoutes(t, pool)
	err := RunMigrations(ctx, pool)
	if !isLockTimeout(err) || !strings.Contains(err.Error(), fmt.Sprintf("pid=%d", blockerPID)) || !strings.Contains(err.Error(), "2 attempts") {
		t.Fatalf("got %v, want a 55P03 naming blocker pid=%d after 2 attempts", err, blockerPID)
	}
	var recorded bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)", drop004).Scan(&recorded); err != nil || recorded {
		t.Fatalf("timed-out migration recorded: %v", err)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("after the blocker left: %v", err)
	}
	conns := make([]*pgxpool.Conn, 0, 4) // schemaPool's MaxConns: the migration connection is one of them
	defer func() {
		for _, c := range conns {
			c.Release()
		}
	}()
	for range 4 {
		c, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
		var v string
		if err := c.QueryRow(ctx, "SHOW lock_timeout").Scan(&v); err != nil || v != "0" {
			t.Fatalf("pooled connection kept lock_timeout %q: %v", v, err)
		}
	}
}

func TestRunMigrationsLockRetryPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	rearm004(t, pool)
	shortLockWaits(t, 300*time.Millisecond, 1500*time.Millisecond)
	blocker, _ := lockKnownRoutes(t, pool)
	released := make(chan struct{})
	t.Cleanup(func() { <-released }) // runs before lockKnownRoutes' Rollback cleanup
	go func() {
		defer close(released)
		time.Sleep(800 * time.Millisecond)
		_ = blocker.Rollback(ctx)
	}()
	start := time.Now()
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("retry after the blocker left: %v", err)
	}
	if waited := time.Since(start); waited < 1500*time.Millisecond {
		t.Fatalf("applied after %s without a retry", waited)
	}
}

// A CIC cancelled while waiting for an old snapshot leaves an invalid index; the retry must rebuild it.
func TestRunMigrationsLockRetryRebuildsIndexPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if _, err := pool.Exec(ctx, "CREATE TABLE probe (v int); CREATE TABLE schema_migrations (filename text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"); err != nil {
		t.Fatal(err)
	}
	shortLockWaits(t, 300*time.Millisecond, 1500*time.Millisecond)
	snapshot, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Rollback(context.Background()) })
	if _, err := snapshot.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	t.Cleanup(func() { <-released }) // runs before the Rollback cleanup above
	go func() {
		defer close(released)
		time.Sleep(800 * time.Millisecond)
		_ = snapshot.Rollback(context.Background())
	}()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	defer conn.Exec(context.Background(), "RESET lock_timeout")
	if _, err := conn.Exec(ctx, "SET lock_timeout = 300"); err != nil {
		t.Fatal(err)
	}
	var pid int32
	if err := conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := applyWithLockRetry(ctx, conn, pool, pid, "905_probe.sql", "CREATE INDEX CONCURRENTLY probe_idx ON probe (v)"); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(start); waited < 1500*time.Millisecond {
		t.Fatalf("applied after %s without a retry", waited)
	}
	var valid bool
	if err := pool.QueryRow(ctx, "SELECT indisvalid AND indisready FROM pg_index WHERE indexrelid = to_regclass('probe_idx')").Scan(&valid); err != nil || !valid {
		t.Fatalf("probe_idx not rebuilt valid: %v", err)
	}
}

// The blocker watch borrows a second pooled connection; a one-connection pool must still migrate.
func TestRunMigrationsSingleConnectionPoolPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	shortLockWaits(t, 30*time.Second, 10*time.Second)
	cfg := pool.Config()
	cfg.MaxConns = 1
	one, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	done := make(chan error, 1)
	go func() { done <- RunMigrations(ctx, one) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("RunMigrations stalled on a one-connection pool")
	}
}
