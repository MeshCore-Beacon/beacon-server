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

func (s *Store) PruneZoneBoundaries(ctx context.Context, keep []string) ([]string, error) {
	if keep == nil {
		keep = []string{}
	}
	return s.q.PruneZoneBoundaries(ctx, keep)
}

func (s *Store) ListZoneBoundaries(ctx context.Context) ([]meshmapper.Boundary, error) {
	rows, err := s.q.ListZoneBoundaries(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]meshmapper.Boundary, 0, len(rows))
	for _, row := range rows {
		b := meshmapper.Boundary{IATA: row.Iata, URL: row.Url, Feature: row.Feature, CheckedAt: row.CheckedAt.Time,
			AttemptedAt: row.AttemptedAt.Time, NextAttempt: row.NextAttempt.Time, LastError: row.LastError}
		if row.Etag != nil {
			b.ETag = *row.Etag
		}
		out = append(out, b)
	}
	return out, nil
}

func (s *Store) SaveZoneBoundary(ctx context.Context, b meshmapper.Boundary) error {
	params := sqlc.SaveZoneBoundaryParams{
		Iata: b.IATA, Url: b.URL, Feature: b.Feature, LastError: b.LastError,
		CheckedAt:   pgtype.Timestamptz{Time: b.CheckedAt, Valid: !b.CheckedAt.IsZero()},
		AttemptedAt: pgtype.Timestamptz{Time: b.AttemptedAt, Valid: true},
		NextAttempt: pgtype.Timestamptz{Time: b.NextAttempt, Valid: true},
	}
	if !b.CheckedAt.IsZero() {
		params.Etag = &b.ETag
	}
	return s.q.SaveZoneBoundary(ctx, params)
}
