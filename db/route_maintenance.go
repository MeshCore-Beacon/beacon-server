// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RouteKey is a keyset position on known_routes (ms-truncated last_seen, id); a zero TS means -infinity.
type RouteKey struct {
	TS time.Time
	ID int64
}

// GraceSliceResult reports one DeleteGraceRoutesSlice batch.
type GraceSliceResult struct {
	Candidates, Deleted int64
	Last                RouteKey
	Unresolved          *time.Time // earliest candidate left behind (locked, or changed since the scan)
	WindowEnd           time.Time  // LEAST(ceil_ms(cutoff), after.TS + 6h): the statement's upper bound
}

// GraceWindow is the widest slice of last_seen one DeleteGraceRoutesSlice call scans. It must
// match the interval '6 hours' in DeleteGraceRoutesSlice (db/queries/queries.sql).
const GraceWindow = 6 * time.Hour

// ceilMs rounds t up to the next whole millisecond.
func ceilMs(t time.Time) time.Time {
	f := t.Truncate(time.Millisecond)
	if f.Before(t) {
		return f.Add(time.Millisecond)
	}
	return f
}

// cutoffCeil is the ms ceiling of the cutoff as Postgres receives it (pgx floors to µs): no row
// with last_seen < cutoff has a truncated last_seen at or past it.
func cutoffCeil(cutoff time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: ceilMs(cutoff.Truncate(time.Microsecond)), Valid: true}
}

func (k RouteKey) ts() pgtype.Timestamptz {
	if k.TS.IsZero() {
		return pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}
	}
	return pgtype.Timestamptz{Time: k.TS, Valid: true}
}

func routeKey(ts pgtype.Timestamptz, id int64) RouteKey {
	if !ts.Valid {
		return RouteKey{}
	}
	return RouteKey{TS: ts.Time, ID: id}
}

// DeleteExpiredRoutes deletes one batch of routes last seen before retentionCutoff, in keyset
// order after after. selected < batch means the run is done; last is the next call's after.
func (s *Store) DeleteExpiredRoutes(ctx context.Context, after RouteKey, retentionCutoff time.Time, batch int32) (selected int64, last RouteKey, err error) {
	row, err := s.q.DeleteExpiredRoutes(ctx, sqlc.DeleteExpiredRoutesParams{
		AfterTs:         after.ts(),
		AfterID:         after.ID,
		RetentionCeil:   cutoffCeil(retentionCutoff),
		RetentionCutoff: pgtype.Timestamptz{Time: retentionCutoff, Valid: true},
		BatchSize:       batch,
	})
	if err != nil {
		return 0, RouteKey{}, err
	}
	return row.Selected, routeKey(row.LastTs, row.LastID), nil
}

// DeleteGraceRoutesSlice deletes one batch of routes last seen before graceCutoff with fewer than
// minObs observations, after after and below WindowEnd (at most GraceWindow past after.TS).
// after.TS must be finite: from -infinity the window would never advance.
func (s *Store) DeleteGraceRoutesSlice(ctx context.Context, after RouteKey, graceCutoff time.Time, minObs int64, batch int32) (GraceSliceResult, error) {
	if after.TS.IsZero() {
		return GraceSliceResult{}, errors.New("grace slice needs a finite start")
	}
	row, err := s.q.DeleteGraceRoutesSlice(ctx, sqlc.DeleteGraceRoutesSliceParams{
		GraceCeil:       cutoffCeil(graceCutoff),
		AfterTs:         after.ts(),
		AfterID:         after.ID,
		GraceCutoff:     pgtype.Timestamptz{Time: graceCutoff, Valid: true},
		MinObservations: minObs,
		BatchSize:       batch,
	})
	if err != nil {
		return GraceSliceResult{}, err
	}
	res := GraceSliceResult{
		Candidates: row.Candidates,
		Deleted:    row.Deleted,
		Last:       routeKey(row.LastTs, row.LastID),
		WindowEnd:  row.WindowEnd.Time,
	}
	if row.UnresolvedTs.Valid {
		u := row.UnresolvedTs.Time
		res.Unresolved = &u
	}
	return res, nil
}

// ReconfirmCursorKey is the background_state key of the reconfirm sweep's position.
const ReconfirmCursorKey = "reconfirm_cursor"

// Test hooks.
var (
	reconfirmBatchSize        = 1000
	reconfirmStatementTimeout = 5 * time.Second
)

// createReconfirmAmb must match db/sqlc_scratch/reconfirm_amb.sql.
const createReconfirmAmb = "CREATE TEMP TABLE reconfirm_amb (iata text NOT NULL, len integer NOT NULL, prefix bytea)"

// ReconfirmResult reports one ReconfirmSweep call.
type ReconfirmResult struct {
	Scanned, Deleted, Skipped int64
	TimedOut                  int
	Wrapped                   bool // the walk passed the last route; the next call starts a new cycle
}

// reconfirmCursor is the sweep's position in known_routes primary-key order.
type reconfirmCursor struct {
	IATA           string    `json:"iata"`
	PathKeyHex     *string   `json:"path_key_hex"` // absent or invalid → restart at ('', '')
	CycleStartedAt time.Time `json:"cycle_started_at"`
	Cycles         int64     `json:"cycles"`
}

// position is the key the next batch starts after. It is never (non-empty iata, nil key), and the
// key is never nil.
func (c reconfirmCursor) position() (iata string, key []byte) {
	if c.IATA == "" || c.PathKeyHex == nil {
		return "", []byte{}
	}
	k, err := hex.DecodeString(*c.PathKeyHex)
	if err != nil {
		return "", []byte{}
	}
	return c.IATA, k
}

// at moves the cursor to (iata, key) within the current cycle.
func (c reconfirmCursor) at(iata string, key []byte) reconfirmCursor {
	h := hex.EncodeToString(key)
	c.IATA, c.PathKeyHex = iata, &h
	return c
}

// wrapped starts the next cycle before the first route.
func (c reconfirmCursor) wrapped(now time.Time) reconfirmCursor {
	empty := ""
	return reconfirmCursor{IATA: "", PathKeyHex: &empty, CycleStartedAt: now, Cycles: c.Cycles + 1}
}

func isStatementTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "57014"
}

// ReconfirmSweep walks up to budget routes after the saved cursor in primary-key order, deleting
// those with a departed hop node or a hop prefix that matches more than one node in the route's
// IATA. It writes no stamp and stops at the end of the table (Wrapped). Each batch commits with
// its cursor; a batch that hits the statement timeout twice is skipped until the next cycle.
func (s *Store) ReconfirmSweep(ctx context.Context, budget int) (res ReconfirmResult, err error) {
	var cur reconfirmCursor
	found, err := s.LoadState(ctx, ReconfirmCursorKey, &cur)
	if err != nil {
		return res, err
	}
	if !found {
		cur = reconfirmCursor{CycleStartedAt: time.Now()}
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return res, err
	}
	defer func() {
		// A connection that cannot drop its temp table is closed, taking the table with it.
		if _, err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS pg_temp.reconfirm_amb"); err != nil {
			_ = conn.Hijack().Close(context.Background())
			return
		}
		conn.Release()
	}()
	q := sqlc.New(conn)
	if _, err := conn.Exec(ctx, createReconfirmAmb); err != nil {
		return res, err
	}
	if err := q.FillReconfirmAmb(ctx); err != nil {
		return res, err
	}
	if _, err := conn.Exec(ctx, "CREATE INDEX ON reconfirm_amb (iata, len, prefix)"); err != nil {
		return res, err
	}
	if _, err := conn.Exec(ctx, "ANALYZE reconfirm_amb"); err != nil {
		return res, err
	}

	for remaining := budget; remaining > 0; {
		n := min(reconfirmBatchSize, remaining)
		row, next, err := reconfirmBatch(ctx, conn, cur, n)
		if err != nil && isStatementTimeout(err) && ctx.Err() == nil {
			res.TimedOut++
			n = max(1, n/10)
			row, next, err = reconfirmBatch(ctx, conn, cur, n)
			if err != nil && isStatementTimeout(err) && ctx.Err() == nil {
				res.TimedOut++
				next, wrapped, err := skipReconfirmSlice(ctx, q, cur, n)
				if err != nil {
					return res, err
				}
				slog.Warn("reconfirm slice skipped after statement timeouts", "component", "db", "iata", cur.IATA, "rows", n)
				res.Skipped += int64(n)
				remaining -= n
				cur = next
				if wrapped {
					res.Wrapped = true
					return res, nil
				}
				continue
			}
		}
		if err != nil {
			return res, err
		}
		res.Scanned += row.Scanned
		res.Deleted += row.Deleted
		remaining -= n
		cur = next
		if row.Scanned < int64(n) {
			res.Wrapped = true
			return res, nil
		}
	}
	return res, nil
}

// reconfirmBatch runs one batch of n routes after cur and saves the next cursor in the same
// transaction. The statement timeout is transaction-local: a Go context timeout would close the
// connection and reconfirm_amb with it.
func reconfirmBatch(ctx context.Context, conn *pgxpool.Conn, cur reconfirmCursor, n int) (sqlc.ReconfirmRoutesBatchRow, reconfirmCursor, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return sqlc.ReconfirmRoutesBatchRow{}, cur, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, "SELECT set_config('statement_timeout', $1, true)",
		fmt.Sprintf("%dms", reconfirmStatementTimeout.Milliseconds())); err != nil {
		return sqlc.ReconfirmRoutesBatchRow{}, cur, err
	}
	q := sqlc.New(tx)
	iata, key := cur.position()
	row, err := q.ReconfirmRoutesBatch(ctx, sqlc.ReconfirmRoutesBatchParams{AfterIata: iata, AfterKey: key, BatchSize: int32(n)})
	if err != nil {
		return sqlc.ReconfirmRoutesBatchRow{}, cur, err
	}
	next := cur.at(row.LastIata, row.LastKey)
	if row.Scanned < int64(n) {
		next = cur.wrapped(time.Now())
	}
	if err := saveState(ctx, q, ReconfirmCursorKey, next); err != nil {
		return sqlc.ReconfirmRoutesBatchRow{}, cur, err
	}
	if err := tx.Commit(ctx); err != nil {
		return sqlc.ReconfirmRoutesBatchRow{}, cur, err
	}
	return row, next, nil
}

// skipReconfirmSlice saves the cursor past the n routes after cur, without a statement timeout.
// wrapped reports that fewer than n routes were left.
func skipReconfirmSlice(ctx context.Context, q *sqlc.Queries, cur reconfirmCursor, n int) (reconfirmCursor, bool, error) {
	iata, key := cur.position()
	end, err := q.ReconfirmSliceEnd(ctx, sqlc.ReconfirmSliceEndParams{AfterIata: iata, AfterKey: key, OffsetRows: int32(n - 1)})
	next, wrapped := cur.at(end.Iata, end.PathKey), false
	if errors.Is(err, pgx.ErrNoRows) {
		next, wrapped, err = cur.wrapped(time.Now()), true, nil
	}
	if err != nil {
		return cur, false, err
	}
	if err := saveState(ctx, q, ReconfirmCursorKey, next); err != nil {
		return cur, false, err
	}
	return next, wrapped, nil
}
