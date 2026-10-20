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
	"time"

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
	// Statements that refuse to run inside a transaction block.
	nonTransactionalRe = regexp.MustCompile(`(?i)\b(?:CONCURRENTLY|VACUUM)\b`)
)

// migrationLockKey serialises RunMigrations across instances sharing a database.
const migrationLockKey int64 = 0x6265_6163_6f6e // "beacon"

// Migrations run before the API and ingest start, so a lock wait must end in a logged exit, not a hang.
var (
	migrationLockTimeout = 30 * time.Second
	migrationLockBackoff = []time.Duration{10 * time.Second, 20 * time.Second}
	blockerPollInterval  = time.Second
)

// isLockTimeout reports SQLSTATE 55P03 (lock_not_available), which lock_timeout raises.
func isLockTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

const blockersSQL = `SELECT coalesce(string_agg(format('pid=%s state=%s xact_start=%s query=%s',
  b.pid, b.state, b.xact_start, left(b.query, 200)), '; ' ORDER BY b.pid), '')
FROM pg_stat_activity b WHERE b.pid = ANY(pg_blocking_pids($1))`

// watchBlockers polls the sessions blocking pid until stop, which returns the last non-empty sample.
func watchBlockers(ctx context.Context, q execQuerier, pid int32) (stop func() string) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var last string
	go func() {
		defer close(done)
		tick := time.NewTicker(blockerPollInterval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				var s string
				if q.QueryRow(ctx, blockersSQL, pid).Scan(&s) == nil && s != "" {
					last = s
				}
			}
		}
	}()
	return func() string { cancel(); <-done; return last }
}

// applyWithLockRetry applies a migration on conn, retrying lock timeouts after each backoff.
// Retrying is safe: transactional files roll back whole, a CIC cut short leaves an invalid
// index that applyMigration drops and rebuilds, and DROP ... IF EXISTS finishes a cancelled DIC.
func applyWithLockRetry(ctx context.Context, conn, watch execQuerier, pid int32, name, sql string) error {
	for attempt := 1; ; attempt++ {
		stop := watchBlockers(ctx, watch, pid)
		err := applyAndRecord(ctx, conn, name, sql)
		blockers := stop()
		if !isLockTimeout(err) {
			return err
		}
		slog.Warn("migration lock wait timed out", "component", "db", "migration", name, "attempt", attempt, "blockers", blockers)
		if attempt > len(migrationLockBackoff) {
			return fmt.Errorf("lock wait over %s on %d attempts, blocked by [%s]: %w", migrationLockTimeout, attempt, blockers, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(migrationLockBackoff[attempt-1]):
		}
	}
}

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

// applyAndRecord applies a migration and its ledger row. Without CONCURRENTLY both go in
// one simple-protocol Exec, an implicit transaction, so a crash can't apply one without the other.
func applyAndRecord(ctx context.Context, db execQuerier, name, sql string) error {
	record := "INSERT INTO schema_migrations (filename) VALUES ('" + strings.ReplaceAll(name, "'", "''") + "')"
	if !nonTransactionalRe.MatchString(lineCommentRe.ReplaceAllString(sql, "")) {
		_, err := db.Exec(ctx, sql+"\n;\n"+record)
		return err
	}
	if err := applyMigration(ctx, db, sql); err != nil {
		return err
	}
	_, err := db.Exec(ctx, record)
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
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire migration connection: %w", err)
	}
	defer conn.Release()
	// Poll, don't block: a waiter in pg_advisory_lock holds a snapshot that the
	// holder's CREATE INDEX CONCURRENTLY waits on, which deadlocks.
	for {
		var locked bool
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", migrationLockKey).Scan(&locked); err != nil {
			return fmt.Errorf("failed to take migration lock: %w", err)
		}
		if locked {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed to take migration lock: %w", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
	defer func() {
		// Session settings and locks outlive Release; drop the connection if either cleanup fails.
		_, resetErr := conn.Exec(context.Background(), "RESET lock_timeout")
		_, unlockErr := conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockKey)
		if resetErr != nil || unlockErr != nil {
			_ = conn.Conn().Close(context.Background())
		}
	}()
	if _, err := conn.Exec(ctx, fmt.Sprintf("SET lock_timeout = %d", migrationLockTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("failed to set migration lock_timeout: %w", err)
	}
	return runMigrations(ctx, conn, pool)
}

func runMigrations(ctx context.Context, pool, watch execQuerier) error {
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

	var pid int32
	if err := pool.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		return fmt.Errorf("failed to read migration backend pid: %w", err)
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

		if err := applyWithLockRetry(ctx, pool, watch, pid, entry.Name(), string(sql)); err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", entry.Name(), err)
		}

		slog.Info(fmt.Sprintf("applied migration: %s", entry.Name()), "component", "db")
	}

	return nil
}
