// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// idHistogram is the planner's view of a table's ids: the equal-frequency histogram of a unique id column and the
// table's estimated row count. A fill uses it to report progress in rows rather than ids, so an id range that
// retention emptied long ago counts for as little as it costs.
type idHistogram struct {
	bounds []int64 // ascending; each adjacent pair holds the same share of rows
	rows   float64
}

// loadObservationIDHistogram reads packet_observations' id statistics. Before the first ANALYZE there are none:
// the histogram is empty and rows reports !ok.
func loadObservationIDHistogram(ctx context.Context, conn interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (idHistogram, error) {
	var h idHistogram
	err := conn.QueryRow(ctx, `SELECT
  COALESCE((SELECT s.histogram_bounds::text::bigint[] FROM pg_stats s
            WHERE s.schemaname = current_schema() AND s.tablename = 'packet_observations' AND s.attname = 'id'),
           '{}'::bigint[]),
  COALESCE((SELECT c.reltuples FROM pg_class c WHERE c.oid = 'packet_observations'::regclass), 0)::float8`).Scan(&h.bounds, &h.rows)
	return h, err
}

// below is the share of rows with an id at or below x, interpolating linearly inside a bucket.
func (h idHistogram) below(x int64) float64 {
	b := h.bounds
	n := len(b) - 1
	switch {
	case x < b[0]:
		return 0
	case x >= b[n]:
		return 1
	}
	i := 0
	for i < n-1 && x >= b[i+1] {
		i++
	}
	within := 1.0
	if b[i+1] > b[i] {
		within = float64(x-b[i]) / float64(b[i+1]-b[i])
	}
	return (float64(i) + within) / float64(n)
}

// estimate is the estimated number of rows with ids in (after, through]; ok is false without statistics. Ids above
// the histogram's last bound arrived since the last ANALYZE and are new, so dense: each counts as one row.
func (h idHistogram) estimate(after, through int64) (int64, bool) {
	if len(h.bounds) < 2 || h.rows <= 0 {
		return 0, false
	}
	if through <= after {
		return 0, true
	}
	last := h.bounds[len(h.bounds)-1]
	in := h.rows * (h.below(min(through, last)) - h.below(min(after, last)))
	return int64(in) + max(through-max(after, last), 0), true
}

// rowsOrIDs is estimate, or the id count when there are no statistics.
func (h idHistogram) rowsOrIDs(after, through int64) int64 {
	if n, ok := h.estimate(after, through); ok {
		return n
	}
	return max(through-after, 0)
}

// idAtRows is the lowest id x in [after, through] with rowsOrIDs(after, x) >= k: the id about k rows above after.
func (h idHistogram) idAtRows(after, through, k int64) int64 {
	lo, hi := after, through
	for lo < hi {
		mid := lo + (hi-lo)/2
		if h.rowsOrIDs(after, mid) >= k {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

// sliceStart picks the lower bound of a sampled slice in (after, through]: the id u of the way (u in [0, 1))
// through the range's estimated rows, leaving room for slice rows above it, so each row is as likely to be sampled
// as any other however sparse its ids are. Without statistics it picks by ids, as rows and ids then coincide.
func (h idHistogram) sliceStart(after, through, slice int64, u float64) int64 {
	room := h.rowsOrIDs(after, through) - slice
	if room <= 0 {
		return after
	}
	return h.idAtRows(after, through, int64(u*float64(room)))
}

// ObservationRows estimates the observations in an id range from the planner's statistics on packet_observations,
// counting ids where there are none. Progress logs use it, so long empty id ranges count for what they cost.
type ObservationRows struct{ h idHistogram }

// In is the estimated number of observations with ids in (after, through].
func (r ObservationRows) In(after, through int64) int64 { return r.h.rowsOrIDs(after, through) }

// ObservationRows reads the statistics for an estimate; a failed read is logged and estimates by ids.
func (s *Store) ObservationRows(ctx context.Context) ObservationRows {
	if s.pool == nil {
		return ObservationRows{}
	}
	h, err := loadObservationIDHistogram(ctx, s.pool)
	if err != nil {
		slog.Warn("cannot read observation statistics; progress counts ids", "component", "db", "error", err)
		return ObservationRows{}
	}
	return ObservationRows{h}
}
