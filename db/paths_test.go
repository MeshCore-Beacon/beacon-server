// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

type pathBucketQuerier struct {
	sqlc.Querier
	row sqlc.GetPathStatsRow
}

func (q pathBucketQuerier) GetPathStats(context.Context, sqlc.GetPathStatsParams) ([]sqlc.GetPathStatsRow, error) {
	return []sqlc.GetPathStatsRow{q.row}, nil
}

func TestPathStatsRejectsInvalidDatabaseBucket(t *testing.T) {
	for _, row := range []sqlc.GetPathStatsRow{
		{HashBytes: 0}, {HashBytes: 4}, {HashBytes: 1, Entries: -1}, {HashBytes: 1, Entries: 64},
		{HashBytes: 4, IsHourly: true},
	} {
		store := &Store{q: pathBucketQuerier{row: row}}
		if _, err := store.GetPathStats(context.Background(), time.Time{}, time.Time{}, nil); err == nil {
			t.Fatalf("invalid bucket accepted: %+v", row)
		}
	}
}
