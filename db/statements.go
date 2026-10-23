// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// unnamedStatements labels the statements that carry no sqlc name.
const unnamedStatements = "(unnamed)"

type statementsQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// DBStatements sums pg_stat_statements for the current database by sqlc statement name.
// It needs a dedicated pool, so call it on the maintenance store.
func (s *Store) DBStatements(ctx context.Context) ([]api.DBStatement, error) {
	if s.pool == nil {
		return nil, errors.New("statement statistics need a dedicated pool")
	}
	return dbStatements(ctx, s.pool)
}

func dbStatements(ctx context.Context, q statementsQuerier) ([]api.DBStatement, error) {
	// The view lives in the extension's schema, which need not be on this session's search_path.
	var schema string
	err := q.QueryRow(ctx, "SELECT extnamespace::regnamespace::text FROM pg_extension WHERE extname = 'pg_stat_statements'").Scan(&schema)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, api.ErrStatementsUnavailable
	}
	if err != nil {
		return nil, fmt.Errorf("find pg_stat_statements: %w", err)
	}
	// regnamespace::text is already quoted where the name needs it.
	rows, err := q.Query(ctx, `-- name: ListDBStatements :many
SELECT query, calls, total_exec_time, total_plan_time, shared_blks_hit, shared_blks_read, rows, wal_bytes::bigint
FROM `+schema+`.pg_stat_statements
WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())`)
	if err != nil {
		return nil, statementsError(err)
	}
	defer rows.Close()
	byName := map[string]*api.DBStatement{}
	for rows.Next() {
		var (
			query          string
			execMs, planMs float64
			r              api.DBStatement
		)
		if err := rows.Scan(&query, &r.Calls, &execMs, &planMs, &r.SharedHit, &r.SharedRead, &r.Rows, &r.WALBytes); err != nil {
			return nil, err
		}
		name := sqlcName(query)
		if name == "" {
			name = unnamedStatements
		}
		sum := byName[name]
		if sum == nil {
			sum = &api.DBStatement{Name: name}
			byName[name] = sum
		}
		sum.Calls += r.Calls
		sum.ExecMs += execMs
		sum.PlanMs += planMs
		sum.SharedHit += r.SharedHit
		sum.SharedRead += r.SharedRead
		sum.Rows += r.Rows
		sum.WALBytes += r.WALBytes
	}
	if err := rows.Err(); err != nil {
		return nil, statementsError(err)
	}
	items := make([]api.DBStatement, 0, len(byName))
	for _, sum := range byName {
		sum.ExecMs = roundMicros(sum.ExecMs)
		sum.PlanMs = roundMicros(sum.PlanMs)
		items = append(items, *sum)
	}
	slices.SortFunc(items, func(a, b api.DBStatement) int {
		if c := cmp.Compare(b.ExecMs, a.ExecMs); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})
	return items, nil
}

// roundMicros rounds milliseconds to whole microseconds.
func roundMicros(ms float64) float64 { return math.Round(ms*1000) / 1000 }

// statementsError maps a missing view (42P01) and a library that was not preloaded (55000)
// to api.ErrStatementsUnavailable.
func statementsError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "42P01" || pgErr.Code == "55000") {
		return api.ErrStatementsUnavailable
	}
	return fmt.Errorf("read pg_stat_statements: %w", err)
}
