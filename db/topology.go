// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package db

import (
	"context"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"time"
)

func (s *Store) GetTopologyLinks(ctx context.Context, iatas []string, since, until time.Time) (*api.TopologyLinks, error) {
	const limit = 100000
	rows, err := s.q.GetTopologyLinks(ctx, sqlc.GetTopologyLinksParams{Iatas: iatas, Since: pgtype.Timestamptz{Time: since, Valid: true}, Until: pgtype.Timestamptz{Time: until, Valid: true}, LinkLimit: limit + 1})
	if err != nil {
		return nil, err
	}
	result := &api.TopologyLinks{Links: make([][2]uuid.UUID, 0, min(len(rows), limit)), Capped: len(rows) > limit, Since: since.UnixMilli(), Until: until.UnixMilli()}
	for _, r := range rows[:min(len(rows), limit)] {
		result.Links = append(result.Links, [2]uuid.UUID{r.FromID, r.ToID})
	}
	return result, nil
}
