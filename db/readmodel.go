// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// Advisory lock classes, taken as pg_try_advisory_lock(class, hashtext(current_schema())), so instances sharing a
	// schema serialise and schemas (tests) do not. Distinct from the migration key and the rollup lock (25189, 1).
	cmiReconcileLockKey int32 = 0x636d69 // "cmi"
	// Reserved for the packet_sites read model.
	packetSitesReconcileLockKey int32 = 0x7073 // "ps"
)

// ErrPreBootWriters means a transaction that began before boot still holds an xid: rows it writes may bypass the
// read models, so the boot repair waits for it.
var ErrPreBootWriters = errors.New("transactions from before boot still hold an xid")

// DatabaseNow returns statement_timestamp() from the database, so the boot cut-off and xact_start share one clock.
func DatabaseNow(ctx context.Context, pool *pgxpool.Pool) (time.Time, error) {
	var now time.Time
	err := pool.QueryRow(ctx, "SELECT statement_timestamp()").Scan(&now)
	return now, err
}

// WaitForPreBootWriters polls every 500 ms until no other session of this database that started its transaction
// before bootTS holds an xid; after maxWait it returns ErrPreBootWriters (wrapped with the count and pids).
// A session of another role is visible only to a privileged role; such a writer is not waited for, and the
// periodic reconcile repairs what it wrote.
func WaitForPreBootWriters(ctx context.Context, pool *pgxpool.Pool, bootTS time.Time, maxWait time.Duration) error {
	deadline := time.Now().Add(maxWait)
	for {
		var n int
		var pids []int32
		if err := pool.QueryRow(ctx, `SELECT count(*), coalesce(array_agg(pid ORDER BY pid), '{}') FROM pg_stat_activity
WHERE datname = current_database() AND backend_xid IS NOT NULL AND xact_start < $1 AND pid <> pg_backend_pid()`, bootTS).Scan(&n, &pids); err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%w: %d session(s), pids %v", ErrPreBootWriters, n, pids)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(min(500*time.Millisecond, time.Until(deadline))):
		}
	}
}
