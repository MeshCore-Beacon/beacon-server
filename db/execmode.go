// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// unnamedDBTX runs every statement as an unnamed prepared statement (pgx.QueryExecModeCacheDescribe):
// pgx caches only the description, never a named server-side statement, so Postgres cannot switch it
// to a generic plan after five executions. Class C statements, whose best plan depends on the values,
// go through it (Store.qc).
//
// Unnamed statements are planned with their values on every call under plan_cache_mode auto or
// force_custom_plan. force_generic_plan still makes them generic, so S2a must never route class C
// statements to a generic pool.
type unnamedDBTX struct{ sqlc.DBTX }

func (u unnamedDBTX) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return u.DBTX.Exec(ctx, sql, append([]any{pgx.QueryExecModeCacheDescribe}, args...)...)
}

func (u unnamedDBTX) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return u.DBTX.Query(ctx, sql, append([]any{pgx.QueryExecModeCacheDescribe}, args...)...)
}

func (u unnamedDBTX) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return u.DBTX.QueryRow(ctx, sql, append([]any{pgx.QueryExecModeCacheDescribe}, args...)...)
}
