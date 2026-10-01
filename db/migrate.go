// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// execQuerier is the slice of pgxpool.Pool / pgx.Conn migrations need; it lets
// integration tests drive applyMigration over a plain connection.
type execQuerier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var (
	lineCommentRe     = regexp.MustCompile(`(?m)--[^\n]*`)
	concurrentIndexRe = regexp.MustCompile(`(?is)\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\s+(?:IF\s+NOT\s+EXISTS\s+)?("?[\w.]+"?)`)
)

// concurrentIndexName returns the index a CREATE INDEX CONCURRENTLY migration builds.
func concurrentIndexName(sql string) (string, bool) {
	m := concurrentIndexRe.FindStringSubmatch(lineCommentRe.ReplaceAllString(sql, ""))
	if m == nil {
		return "", false
	}
	return strings.Trim(m[1], `"`), true
}

// isDuplicateRelation reports SQLSTATE 42P07 (relation already exists).
func isDuplicateRelation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P07"
}

// applyMigration runs one file outside a transaction. An interrupted CREATE INDEX
// CONCURRENTLY leaves an invalid index that trips 42P07 on retry; drop it and rebuild once.
func applyMigration(ctx context.Context, db execQuerier, sql string) error {
	_, err := db.Exec(ctx, sql)
	if err == nil || !isDuplicateRelation(err) {
		return err
	}
	name, ok := concurrentIndexName(sql)
	if !ok {
		return err
	}
	ident := pgx.Identifier(strings.Split(name, ".")).Sanitize()
	var valid bool
	if scanErr := db.QueryRow(ctx,
		"SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)", ident,
	).Scan(&valid); scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return err
		}
		return fmt.Errorf("%w (checking index %s: %v)", err, name, scanErr)
	}
	if valid {
		slog.Info(fmt.Sprintf("index %s already built, recording migration", name), "component", "db")
		return nil
	}
	if _, dropErr := db.Exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+ident); dropErr != nil {
		return fmt.Errorf("dropping invalid index %s: %w", name, dropErr)
	}
	slog.Warn(fmt.Sprintf("dropped invalid index %s, rebuilding", name), "component", "db")
	_, err = db.Exec(ctx, sql)
	return err
}

const baselineMigration = "001_baseline.sql"

var errPreBaseline = errors.New("database schema predates Beacon 2.0.0; 2.0.0 needs a fresh database")

// checkBaseline refuses databases built by the 1.x migration chain, which the
// 2.0.0 baseline replaced; applying it on top would collide with existing tables.
func checkBaseline(ctx context.Context, db execQuerier) error {
	var ledger, hasBaseline, hasPackets bool
	err := db.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM schema_migrations),
		       EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1),
		       to_regclass('packets') IS NOT NULL`, baselineMigration,
	).Scan(&ledger, &hasBaseline, &hasPackets)
	if err != nil {
		return fmt.Errorf("failed to check migration baseline: %w", err)
	}
	if (ledger && !hasBaseline) || (!ledger && hasPackets) {
		return errPreBaseline
	}
	return nil
}

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

	if err := checkBaseline(ctx, pool); err != nil {
		return err
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

		if err := applyMigration(ctx, pool, string(sql)); err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", entry.Name(), err)
		}

		if _, err := pool.Exec(
			ctx,
			"INSERT INTO schema_migrations (filename) VALUES ($1)",
			entry.Name(),
		); err != nil {
			return fmt.Errorf("failed to record migration %s: %w", entry.Name(), err)
		}

		slog.Info(fmt.Sprintf("applied migration: %s", entry.Name()), "component", "db")
	}

	return nil
}
