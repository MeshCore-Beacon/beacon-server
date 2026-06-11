// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func RunMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			filename TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create schema_migrations: %w", err)
	}

	// After creating schema_migrations table, check if we need to bootstrap
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema_migrations").Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check migrations count: %w", err)
	}
	if count == 0 {
		// Check if db is already initialized by looking for a known table
		var exists bool
		err = pool.QueryRow(ctx, `
        SELECT EXISTS(
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = 'packets'
        )
    `).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check existing schema: %w", err)
		}
		if exists {
			// Mark 001 as already applied
			if _, err := pool.Exec(
				ctx,
				"INSERT INTO schema_migrations (filename) VALUES ($1)",
				"001_initial_schema.sql",
			); err != nil {
				return fmt.Errorf("failed to bootstrap migrations: %w", err)
			}
			fmt.Println("bootstrapped existing schema as 001_initial_schema.sql")
		}
	}

	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("failed to read migrations: %w", err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		var already bool
		err := pool.QueryRow(
			ctx,
			"SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)",
			entry.Name(),
		).Scan(&already)
		if err != nil {
			return fmt.Errorf("failed to check migration %s: %w", entry.Name(), err)
		}
		if already {
			continue
		}

		sql, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", entry.Name(), err)
		}

		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", entry.Name(), err)
		}

		if _, err := pool.Exec(
			ctx,
			"INSERT INTO schema_migrations (filename) VALUES ($1)",
			entry.Name(),
		); err != nil {
			return fmt.Errorf("failed to record migration %s: %w", entry.Name(), err)
		}

		fmt.Printf("applied migration: %s\n", entry.Name())
	}

	return nil
}
