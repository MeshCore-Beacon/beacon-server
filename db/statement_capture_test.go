// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
)

// statementCapture records the SQL and args of sqlc statements whose name starts with name, for EXPLAIN.
type statementCapture struct {
	pgx.Tx
	name      string
	statement string
	args      []any
}

func (c *statementCapture) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.Contains(sql, "-- name: "+c.name) {
		c.statement, c.args = sql, args
	}
	return c.Tx.Query(ctx, sql, args...)
}
