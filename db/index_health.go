// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"maps"

	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/jackc/pgx/v5"
)

// indexBaselinesKey holds map[index name]indexBaseline in background_state.
const indexBaselinesKey = "index_baselines"

// indexBaseline is an index's bytes per row when first seen or last rebuilt. A rebuild changes
// the filenode, which resets the baseline.
type indexBaseline struct {
	Filenode    int64   `json:"filenode"`
	BytesPerRow float64 `json:"bytes_per_row"`
}

// freshBytesPerRow are the bytes per row of a freshly built packet_sites index (sorted builds on 7.4M rows). That
// table is never built fresh, so its first-seen size would be a half-churned baseline: these indexes start from the
// constant instead, once the table holds freshBaselineMinRows rows (smaller tables give meaningless ratios). A
// rebuild (a new filenode) re-baselines from the measurement as for any index.
var freshBytesPerRow = map[string]float64{
	"packet_sites_pkey":           40.5,
	"idx_packet_sites_site":       59.1,
	"idx_packet_sites_site_type":  49.9,
	"idx_packet_sites_site_scope": 58.4,
}

const freshBaselineMinRows = 100_000

// IndexHealth is one index's size and its bytes per row relative to its baseline. HasRatio is
// false for an index that has never been analyzed.
type IndexHealth struct {
	Name     string
	Bytes    int64
	Ratio    float64
	HasRatio bool
}

// UnusedIndex is a non-unique index with no scans since the last statistics reset.
type UnusedIndex struct {
	Name  string
	Bytes int64
}

// The catalog statements stay raw SQL: sqlc does not know the pg_stat_* views.
const (
	listIndexSizes = `-- name: ListIndexSizes :many
SELECT c.relname::text, pg_relation_filenode(c.oid)::bigint, pg_relation_size(c.oid)::bigint, c.reltuples::float8
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = current_schema()
ORDER BY 1`

	listUnusedIndexes = `-- name: ListUnusedIndexes :many
SELECT s.indexrelname::text, pg_relation_size(s.indexrelid)::bigint AS bytes
FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.schemaname = current_schema() AND s.idx_scan = 0 AND NOT i.indisunique
ORDER BY bytes DESC, 1`

	getPgActivity = `-- name: GetPgActivity :one
SELECT (SELECT wal_bytes FROM pg_stat_wal)::bigint, checkpoints_timed, checkpoints_req,
       (SELECT age(datfrozenxid) FROM pg_database WHERE datname = current_database())::bigint
FROM pg_stat_bgwriter`

	// PostgreSQL 17 moved the checkpoint counters to pg_stat_checkpointer.
	getPgActivity17 = `-- name: GetPgActivity17 :one
SELECT (SELECT wal_bytes FROM pg_stat_wal)::bigint, num_timed, num_requested,
       (SELECT age(datfrozenxid) FROM pg_database WHERE datname = current_database())::bigint
FROM pg_stat_checkpointer`

	getServerVersionNum = "-- name: GetServerVersionNum :one\nSELECT current_setting('server_version_num')::int"
)

// MeasureIndexHealth reads every index in the current schema and compares its bytes per row with
// the baseline taken when the index was first seen or last rebuilt. It saves the baselines only
// when they changed, and drops those of indexes that no longer exist.
func (s *Store) MeasureIndexHealth(ctx context.Context) ([]IndexHealth, error) {
	rows, err := s.pool.Query(ctx, listIndexSizes)
	if err != nil {
		return nil, err
	}
	type indexSize struct {
		name      string
		filenode  int64
		bytes     int64
		reltuples float64
	}
	sizes, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (indexSize, error) {
		var x indexSize
		err := r.Scan(&x.name, &x.filenode, &x.bytes, &x.reltuples)
		return x, err
	})
	if err != nil {
		return nil, err
	}

	baselines := map[string]indexBaseline{}
	if _, err := s.LoadState(ctx, indexBaselinesKey, &baselines); err != nil {
		return nil, err
	}
	next := make(map[string]indexBaseline, len(sizes))
	out := make([]IndexHealth, 0, len(sizes))
	for _, x := range sizes {
		h := IndexHealth{Name: x.name, Bytes: x.bytes}
		b, ok := baselines[x.name]
		if x.reltuples > 0 {
			bpr := float64(x.bytes) / x.reltuples
			fresh, isFresh := freshBytesPerRow[x.name]
			switch {
			case ok && b.Filenode == x.filenode && b.BytesPerRow > 0:
			case isFresh && !ok && x.reltuples < freshBaselineMinRows:
				out = append(out, h) // too small for a ratio; no baseline yet
				continue
			case isFresh && !ok:
				b, ok = indexBaseline{Filenode: x.filenode, BytesPerRow: fresh}, true
			default:
				b, ok = indexBaseline{Filenode: x.filenode, BytesPerRow: bpr}, true
			}
			h.Ratio, h.HasRatio = bpr/b.BytesPerRow, true
		}
		// An index without statistics keeps the baseline it has, and gets none if it has none.
		if ok {
			next[x.name] = b
		}
		out = append(out, h)
	}
	if !maps.Equal(next, baselines) {
		if err := s.SaveState(ctx, indexBaselinesKey, next); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// UnusedIndexes lists the non-unique indexes in the current schema with no scans since the last
// statistics reset, largest first.
func (s *Store) UnusedIndexes(ctx context.Context) ([]UnusedIndex, error) {
	rows, err := s.pool.Query(ctx, listUnusedIndexes)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (UnusedIndex, error) {
		var u UnusedIndex
		err := r.Scan(&u.Name, &u.Bytes)
		return u, err
	})
}

// PgActivity reads PostgreSQL's WAL, checkpoint and transaction ID age counters. The server
// version, which picks the statement, is read once.
func (s *Store) PgActivity(ctx context.Context) (metrics.PgActivity, error) {
	version := s.pgVersion.Load()
	if version == 0 {
		var v int32
		if err := s.pool.QueryRow(ctx, getServerVersionNum).Scan(&v); err != nil {
			return metrics.PgActivity{}, err
		}
		s.pgVersion.Store(v)
		version = v
	}
	query := getPgActivity
	if version >= 170000 {
		query = getPgActivity17
	}
	var a metrics.PgActivity
	err := s.pool.QueryRow(ctx, query).Scan(&a.WALBytes, &a.CheckpointsTimed, &a.CheckpointsRequested, &a.XIDAge)
	return a, err
}
