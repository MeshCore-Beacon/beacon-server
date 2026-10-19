// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5/pgtype"
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

// GraceWindow is the widest slice of last_seen one DeleteGraceRoutesSlice call scans.
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
func (s *Store) DeleteGraceRoutesSlice(ctx context.Context, after RouteKey, graceCutoff time.Time, minObs int64, batch int32) (GraceSliceResult, error) {
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
