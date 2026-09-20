// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestBackupPreconditionsDegradeWithoutDatabaseOrExit(t *testing.T) {
	cfg := &config.Config{}
	if got := configureBackup(context.Background(), cfg, nil, "", "saved.yaml"); got.ConnectionService != "" {
		t.Fatal("disabled backup configured")
	}
	cfg.Backup.Enabled = true
	if got := configureBackup(context.Background(), cfg, nil, "", "saved.yaml"); got.ConnectionService != "" {
		t.Fatal("missing admin key configured")
	}
	cfg.Auth.APIKey = "test-only-key"
	if got := configureBackup(context.Background(), cfg, nil, "host=localhost user=test dbname=test", "saved.yaml"); got.ConnectionService != "" {
		t.Fatal("keyword connection configured for backup")
	}
}

func TestBackupConfigurationPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" || runtime.GOOS == "windows" {
		t.Skip("requires BEACON_TEST_POSTGRES_DSN and a Unix test client")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("test database configuration unavailable")
	}
	defer pool.Close()
	bin := t.TempDir()
	t.Setenv("PATH", bin)
	cfg := &config.Config{}
	cfg.Backup.Enabled, cfg.Auth.APIKey = true, "test-only-key"
	for _, major := range []string{"999", "15"} {
		if err := os.WriteFile(filepath.Join(bin, "pg_dump"), []byte("#!/bin/sh\necho 'pg_dump (PostgreSQL) "+major+".0'\n"), 0700); err != nil {
			t.Fatal(err)
		}
		got := configureBackup(ctx, cfg, pool, dsn, "saved.yaml")
		if (got.ConnectionService != "") != (major == "999") {
			t.Fatalf("unexpected backup availability for test client major %s", major)
		}
	}
}
