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

// waitDatabase creates a database of its own, so other packages' open transactions never count.
func waitDatabase(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "cmi_wait_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
		admin.Close(context.Background())
	})
	if _, err := pool.Exec(ctx, "CREATE TABLE w (x int)"); err != nil {
		t.Fatal(err)
	}
	return ctx, pool
}

func TestWaitForPreBootWritersPostgres(t *testing.T) {
	ctx, pool := waitDatabase(t)
	conn := func() *pgxpool.Conn {
		c, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Release)
		return c
	}
	begin := func(c *pgxpool.Conn, write bool) pgx.Tx {
		tx, err := c.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		sql := "SELECT 1"
		if write {
			sql = "INSERT INTO w VALUES (1)"
		}
		if _, err := tx.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
		return tx
	}

	reader := begin(conn(), false)              // read-only, from before boot: holds no xid
	defer reader.Rollback(context.Background()) //nolint:errcheck
	writer := begin(conn(), true)
	time.Sleep(10 * time.Millisecond)
	bootTS, err := DatabaseNow(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = WaitForPreBootWriters(ctx, pool, bootTS, time.Second)
	if !errors.Is(err, ErrPreBootWriters) || time.Since(start) < time.Second {
		t.Fatalf("wait = %v after %s, want ErrPreBootWriters after at least 1 s", err, time.Since(start))
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	later := begin(conn(), true)               // a writer that began after boot
	defer later.Rollback(context.Background()) //nolint:errcheck
	start = time.Now()
	if err := WaitForPreBootWriters(ctx, pool, bootTS, time.Second); err != nil || time.Since(start) > time.Second {
		t.Fatalf("wait = %v after %s, want nil at once", err, time.Since(start))
	}
}
