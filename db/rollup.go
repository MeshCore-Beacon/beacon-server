// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Must match db/sqlc_scratch/rollup_obs.sql, which only exists so sqlc can type-check rollup.sql.
const createRollupObs = `CREATE TEMP TABLE rollup_obs (
    packet_hash bytea NOT NULL, observer_id uuid NOT NULL, iata character(3) NOT NULL,
    heard_at timestamptz NOT NULL, path_length_byte smallint NOT NULL, hash_size smallint NOT NULL,
    hop_count smallint NOT NULL, path_len integer NOT NULL, rssi smallint, snr real, airtime_ms real,
    payload_type smallint, route_type smallint NOT NULL, origin_pubkey bytea, scope_id integer
) ON COMMIT DROP`

// rollupDelay is how long after an hour closes it becomes eligible to roll (ingest clamps
// heard_at to ±30 min; RegisterRollupHours uses the same bound).
const rollupDelay = 95 * time.Minute

// RollOutcome is what RollHour did with an hour.
type RollOutcome int

const (
	RollSkipped  RollOutcome = iota // unregistered, or lost a serialization race; retried next pass
	RollPartial                     // raw rows already deleted; never rolled
	RollComplete                    // rolled and marked complete
)

// RollupSession holds the rollup advisory lock on one dedicated connection.
type RollupSession struct {
	conn *pgxpool.Conn
	q    *sqlc.Queries
}

// BeginRollup takes the rollup lock. ok is false when another process holds it.
func (s *Store) BeginRollup(ctx context.Context) (*RollupSession, bool, error) {
	if s.pool == nil {
		return nil, false, errors.New("rollup: store has no connection pool")
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	q := sqlc.New(conn)
	ok, err := q.TryRollupLock(ctx)
	if err != nil || !ok {
		conn.Release()
		return nil, false, err
	}
	return &RollupSession{conn: conn, q: q}, true, nil
}

// Close releases the lock. If unlocking fails the connection is closed so the lock can't leak.
func (r *RollupSession) Close(ctx context.Context) {
	if err := r.q.RollupUnlock(ctx); err != nil {
		_ = r.conn.Hijack().Close(context.Background())
		return
	}
	r.conn.Release()
}

// RegisterHours adds a 'missing' row for every eligible hour since max(since, earliest observation).
func (r *RollupSession) RegisterHours(ctx context.Context, since time.Time) error {
	return r.q.RegisterRollupHours(ctx, ts(since))
}

func (r *RollupSession) MarkPartialHours(ctx context.Context) error {
	return r.q.MarkPartialRollupHours(ctx)
}

// MissingHours returns up to limit unrolled hours, oldest first.
func (r *RollupSession) MissingHours(ctx context.Context, limit int32) ([]time.Time, error) {
	rows, err := r.q.ListMissingRollupHours(ctx, limit)
	return times(rows), err
}

// DirtyHours drops dirty hours that can't be re-rolled and returns up to limit of the rest.
func (r *RollupSession) DirtyHours(ctx context.Context, limit int32) ([]time.Time, error) {
	if err := r.q.DeleteStaleDirtyHours(ctx); err != nil {
		return nil, err
	}
	rows, err := r.q.ListDirtyRollupHours(ctx, limit)
	return times(rows), err
}

// RollHour rebuilds every rollup family for one hour in a single REPEATABLE READ transaction.
// changed reports whether the hour's content differs from its previous roll.
func (r *RollupSession) RollHour(ctx context.Context, hour time.Time) (outcome RollOutcome, changed bool, err error) {
	tx, err := r.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return RollSkipped, false, err
	}
	defer func() {
		_ = tx.Rollback(context.Background())
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "40001" {
			outcome, changed, err = RollSkipped, false, nil
		}
	}()
	outcome, changed, err = rollHour(ctx, tx, hour)
	if err != nil {
		return RollSkipped, false, err
	}
	return outcome, changed, tx.Commit(ctx)
}

// rollHour does RollHour's work inside tx; the caller commits.
func rollHour(ctx context.Context, tx pgx.Tx, hour time.Time) (RollOutcome, bool, error) {
	q := sqlc.New(tx)
	h := ts(hour)
	status, err := q.LockRollupHour(ctx, h)
	if errors.Is(err, pgx.ErrNoRows) {
		return RollSkipped, false, nil
	}
	if err != nil {
		return RollSkipped, false, err
	}
	if status == "partial" {
		return RollPartial, false, q.SetRollupHourPartial(ctx, h)
	}
	if _, err := tx.Exec(ctx, createRollupObs); err != nil {
		return RollSkipped, false, err
	}
	if err := q.FillRollupObs(ctx, h); err != nil {
		return RollSkipped, false, err
	}
	if _, err := tx.Exec(ctx, "ANALYZE rollup_obs"); err != nil {
		return RollSkipped, false, err
	}
	if err := q.DeleteRollupHour(ctx, h); err != nil {
		return RollSkipped, false, err
	}
	for name, roll := range map[string]func(context.Context, pgtype.Timestamptz) error{
		"iata observations": q.RollIATAObservations,
		"payload breakdown": q.RollPayloadBreakdown,
		"signal":            q.RollSignal,
		"paths":             q.RollPaths,
		"observer activity": q.RollObserverActivity,
		"observer identity": q.RollObserverIdentity,
		"advert hearings":   q.RollAdvertHearings,
		"advert sets":       q.RollAdvertSets,
		"talker sets":       q.RollTalkerSets,
		"packet sets":       q.RollPacketSets,
		"scope sets":        q.RollScopeSets,
		"scope observers":   q.RollScopeObservers,
		"scope nodes":       q.RollScopeNodes,
	} {
		if err := roll(ctx, h); err != nil {
			return RollSkipped, false, fmt.Errorf("roll %s: %w", name, err)
		}
	}
	// Dropped here as well as ON COMMIT so a caller can roll several hours in one transaction.
	if _, err := tx.Exec(ctx, "DROP TABLE rollup_obs"); err != nil {
		return RollSkipped, false, err
	}
	hash, err := q.RollupContentHash(ctx, h)
	if err != nil {
		return RollSkipped, false, err
	}
	changed, err := q.FinishRollupHour(ctx, sqlc.FinishRollupHourParams{Hour: h, ContentHash: hash})
	if err != nil {
		return RollSkipped, false, err
	}
	return RollComplete, changed, nil
}

// AnalyticsRevision changes whenever rolled content, an hour's status or rollup coverage changes.
func (s *Store) AnalyticsRevision(ctx context.Context) (int64, error) {
	return s.q.GetAnalyticsRevision(ctx)
}

func ts(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
}

func times(rows []pgtype.Timestamptz) []time.Time {
	out := make([]time.Time, len(rows))
	for i, r := range rows {
		out[i] = r.Time
	}
	return out
}

// OldestMissingRollupHour returns the earliest hour still waiting to be rolled; ok is false if none.
func (s *Store) OldestMissingRollupHour(ctx context.Context) (hour time.Time, ok bool, err error) {
	h, err := s.q.OldestMissingRollupHour(ctx)
	return h.Time, h.Valid, err
}

// DeleteOldRollups drops rolled hours, their bookkeeping and queued re-rolls before cutoff.
func (s *Store) DeleteOldRollups(ctx context.Context, cutoff time.Time) error {
	return s.q.DeleteOldRollups(ctx, ts(cutoff))
}
