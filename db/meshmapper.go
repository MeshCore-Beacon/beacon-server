// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/meshmapper"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) GetScopeCatalogue(ctx context.Context, iata, url string) (*meshmapper.Cache, error) {
	row, err := s.q.GetScopeCatalogue(ctx, sqlc.GetScopeCatalogueParams{Iata: iata, Url: url})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cache := &meshmapper.Cache{Payload: row.Payload, CheckedAt: row.CheckedAt.Time, AttemptedAt: row.AttemptedAt.Time, NextAttempt: row.NextAttempt.Time, LastError: row.LastError}
	if row.Etag != nil {
		cache.ETag = *row.Etag
	}
	return cache, nil
}

func (s *Store) SaveScopeCatalogue(ctx context.Context, iata, url string, cache meshmapper.Cache, entries []scopestore.Entry) error {
	params := sqlc.SaveScopeCatalogueParams{
		Iata: iata, Url: url, Payload: cache.Payload, LastError: cache.LastError,
		CheckedAt:   pgtype.Timestamptz{Time: cache.CheckedAt, Valid: !cache.CheckedAt.IsZero()},
		AttemptedAt: pgtype.Timestamptz{Time: cache.AttemptedAt, Valid: true},
		NextAttempt: pgtype.Timestamptz{Time: cache.NextAttempt, Valid: true},
		Names:       []string{}, Keys: [][]byte{}, Fingerprints: [][]byte{},
	}
	if !cache.CheckedAt.IsZero() {
		params.Etag = &cache.ETag
	}
	for _, entry := range entries {
		params.Names = append(params.Names, entry.Name)
		params.Keys = append(params.Keys, entry.TransportKey)
		params.Fingerprints = append(params.Fingerprints, entry.KeyFingerprint)
	}
	return s.q.SaveScopeCatalogue(ctx, params)
}
