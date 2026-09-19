// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) GetObserverComparison(ctx context.Context, a, b uuid.UUID, since, until time.Time, iatas []string) (*api.ObserverComparison, error) {
	row, err := s.q.GetObserverComparison(ctx, sqlc.GetObserverComparisonParams{
		ObserverA: a, ObserverB: b,
		Since: pgtype.Timestamptz{Time: since, Valid: true},
		Until: pgtype.Timestamptz{Time: until, Valid: true},
		Iatas: iatas,
	})
	if err != nil {
		return nil, err
	}
	if !row.KnownA || !row.KnownB {
		return nil, pgx.ErrNoRows
	}
	return &api.ObserverComparison{
		ObserverA: a, ObserverB: b, Since: since.UnixMilli(), Until: until.UnixMilli(),
		TotalPackets: row.TotalPackets, OnlyA: row.OnlyA, OnlyB: row.OnlyB, Both: row.Both,
	}, nil
}
