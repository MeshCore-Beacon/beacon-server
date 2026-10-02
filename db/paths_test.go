// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5/pgtype"
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

type pathRowsQuerier struct {
	sqlc.Querier
	rows []sqlc.GetPathStatsRow
}

func (q pathRowsQuerier) GetPathStats(context.Context, sqlc.GetPathStatsParams) ([]sqlc.GetPathStatsRow, error) {
	return q.rows, nil
}

// An hour's maxEntries is the largest across its category/width rows, not the last one seen.
func TestPathStatsHourlyMaxEntries(t *testing.T) {
	hour := pgtype.Timestamptz{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	store := &Store{q: pathRowsQuerier{rows: []sqlc.GetPathStatsRow{
		{IsHourly: true, Hour: hour, Category: 0, HashBytes: 1, Receptions: 3, MaxEntries: 9},
		{IsHourly: true, Hour: hour, Category: 0, HashBytes: 2, Receptions: 1, MaxEntries: 5},
		{IsHourly: true, Hour: hour, Category: 1, HashBytes: 0, Receptions: 2, MaxEntries: 0},
	}}}
	got, err := store.GetPathStats(context.Background(), time.Time{}, time.Time{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Hourly) != 1 || got.Hourly[0].MaxEntries != 9 {
		t.Errorf("hourly %+v, want one hour with maxEntries 9", got.Hourly)
	}
}
