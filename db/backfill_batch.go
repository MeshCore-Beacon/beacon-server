// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const backfillStatementTimeout = 5 * time.Second

// Only one boot backfill writes at a time. Separate maintenance connections do
// not isolate the disk/WAL used by live ingest.
var backfillWriteSlot = make(chan struct{}, 1)

// backfillWrite bounds one idempotent write with a server-side timeout. A local
// transaction restores the setting on success or failure without releasing the
// session advisory lock. In particular, a canceled statement must be rolled back
// before the caller retries an FK race or a smaller range.
func backfillWrite(ctx context.Context, conn *pgxpool.Conn, timeout time.Duration, write func() (int64, error)) (int64, error) {
	select {
	case backfillWriteSlot <- struct{}{}:
		defer func() { <-backfillWriteSlot }()
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	// Never relax an operator's smaller timeout. pg_settings reports milliseconds.
	if _, err := tx.Exec(ctx, `SELECT set_config('statement_timeout',
		CASE WHEN setting::bigint = 0 THEN ($1::bigint)::text
		     ELSE LEAST(setting::bigint, $1::bigint)::text END, true)
		FROM pg_settings WHERE name = 'statement_timeout'`, max(1, timeout.Milliseconds())); err != nil {
		return 0, err
	}
	n, err := write()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

func backfillTimedOut(ctx context.Context, err error) bool {
	return ctx.Err() == nil && isStatementTimeout(err)
}

func backfillTimeout(limit time.Duration, deadline time.Time) time.Duration {
	if limit <= 0 {
		limit = backfillStatementTimeout
	}
	return max(time.Millisecond, min(limit, time.Until(deadline)))
}
