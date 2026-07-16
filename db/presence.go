// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) TouchObservers(ctx context.Context, ids []uuid.UUID, seen []time.Time, counts []int32) error {
	return s.q.TouchObservers(ctx, sqlc.TouchObserversParams{
		Column1: ids,
		Column2: toTimestamptzs(seen),
		Column3: counts,
	})
}

func (s *Store) TouchObserverBrokers(ctx context.Context, ids []uuid.UUID, brokers []string, seen []time.Time) error {
	return s.q.TouchObserverBrokers(ctx, sqlc.TouchObserverBrokersParams{
		Column1: ids,
		Column2: brokers,
		Column3: toTimestamptzs(seen),
	})
}

func (s *Store) TouchPackets(ctx context.Context, hashes [][]byte, heard []time.Time) error {
	return s.q.TouchPackets(ctx, sqlc.TouchPacketsParams{
		Column1: hashes,
		Column2: toTimestamptzs(heard),
	})
}

func toTimestamptzs(ts []time.Time) []pgtype.Timestamptz {
	out := make([]pgtype.Timestamptz, len(ts))
	for i, t := range ts {
		out[i] = pgtype.Timestamptz{Time: t, Valid: true}
	}
	return out
}
