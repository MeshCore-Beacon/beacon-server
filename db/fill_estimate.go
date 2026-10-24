// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"

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
