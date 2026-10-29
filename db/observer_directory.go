// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/obscount"
	"github.com/jackc/pgx/v5/pgtype"
)

// WithObservationCounts makes the store count every observation it stores into c, and lets the
// observer directory read the hours c covers from it. Give it to the ingesting store and the API
// store of the one process that ingests.
func WithObservationCounts(c *obscount.Counts) Option {
	return func(s *Store) { s.obsCounts = c }
}

// WithObservationActivity has InsertObservation add each new observation to a and observer activity read
// the slots a covers from it. Pass it only in the process that ingests (nil disables it).
func WithObservationActivity(a *obscount.Activity) Option {
	return func(s *Store) { s.obsActivity = a }
}

func (s *Store) ListObserverDirectory(ctx context.Context, query api.ObserverDirectoryQuery) (*api.ObserverDirectory, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	params := sqlc.ListObserverDirectoryParams{
		Column1: ts(time.UnixMilli(query.Since)), Column2: ts(time.UnixMilli(query.Until)), Column3: query.IATAs,
		Column4: query.Broker, Column5: query.Name, Column6: query.Status, Column7: query.Scope, Column8: query.Type, Column9: query.Sort,
		Column10: query.Cursor, Column11: query.Limit, Column12: query.MatchNone,
	}
	if live := s.obsCounts.Snapshot(time.UnixMilli(query.Until)); live.Live() {
		params.Column13, params.Column14 = ts(live.From), ts(live.Until)
		params.Column15 = make([]pgtype.Timestamptz, len(live.Hours))
		for i, h := range live.Hours {
			params.Column15[i] = ts(h)
		}
		params.Column16, params.Column17, params.Column18 = live.Observers, live.IATAs, live.Counts
	}
	raw, err := s.q.ListObserverDirectory(ctx, params)
	if err != nil {
		return nil, err
	}
	var page api.ObserverDirectory
	if err := json.Unmarshal([]byte(raw), &page); err != nil {
		return nil, err
	}
	return &page, nil
}
