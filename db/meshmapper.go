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

func (s *Store) ListScopeCatalogues(ctx context.Context) ([]meshmapper.Catalogue, error) {
	rows, err := s.q.ListScopeCatalogues(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]meshmapper.Catalogue, 0, len(rows))
	for _, row := range rows {
		c := meshmapper.Catalogue{IATA: row.Iata, URL: row.Url, Cache: meshmapper.Cache{Payload: row.Payload, CheckedAt: row.CheckedAt.Time,
			AttemptedAt: row.AttemptedAt.Time, NextAttempt: row.NextAttempt.Time, LastError: row.LastError}}
		if row.Etag != nil {
			c.ETag = *row.Etag
		}
		out = append(out, c)
	}
	return out, nil
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

func (s *Store) ListKnownIATAs(ctx context.Context) ([]string, error) {
	rows, err := s.q.ListIATAs(ctx)
	if err != nil {
		return nil, err
	}
	iatas := make([]string, 0, len(rows))
	for _, row := range rows {
		iatas = append(iatas, row.Iata)
	}
	return iatas, nil
}

// ListHeardIATAs returns the IATAs observers last reported from.
func (s *Store) ListHeardIATAs(ctx context.Context) ([]string, error) {
	return s.q.ListHeardIATAs(ctx)
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

func (s *Store) ListZoneLists(ctx context.Context) ([]meshmapper.ZoneList, error) {
	rows, err := s.q.ListZoneLists(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]meshmapper.ZoneList, 0, len(rows))
	for _, row := range rows {
		l := meshmapper.ZoneList{Country: row.Country, Payload: row.Payload, FetchedAt: row.FetchedAt.Time,
			AttemptedAt: row.AttemptedAt.Time, NextAttempt: row.NextAttempt.Time, LastError: row.LastError}
		if row.Etag != nil {
			l.ETag = *row.Etag
		}
		out = append(out, l)
	}
	return out, nil
}

func (s *Store) SaveZoneList(ctx context.Context, l meshmapper.ZoneList) error {
	params := sqlc.SaveZoneListParams{
		Country: l.Country, Payload: l.Payload, LastError: l.LastError,
		FetchedAt:   pgtype.Timestamptz{Time: l.FetchedAt, Valid: !l.FetchedAt.IsZero()},
		AttemptedAt: pgtype.Timestamptz{Time: l.AttemptedAt, Valid: true},
		NextAttempt: pgtype.Timestamptz{Time: l.NextAttempt, Valid: true},
	}
	if !l.FetchedAt.IsZero() {
		params.Etag = &l.ETag
	}
	return s.q.SaveZoneList(ctx, params)
}

func (s *Store) ListRegionState(ctx context.Context) ([]meshmapper.RegionState, error) {
	rows, err := s.q.ListRegionState(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]meshmapper.RegionState, 0, len(rows))
	for _, row := range rows {
		out = append(out, meshmapper.RegionState{Slug: row.Slug, Name: row.Name, DisplayOrder: int(row.DisplayOrder),
			Imported: row.Imported, IATAs: row.Iatas, CenterLat: row.CenterLat, CenterLng: row.CenterLng})
	}
	return out, nil
}

// SaveImportedRegion reports false when a hand-written region owns the slug.
func (s *Store) SaveImportedRegion(ctx context.Context, r meshmapper.RegionState) (bool, error) {
	order := int32(r.DisplayOrder)
	id, err := s.q.UpsertImportedRegion(ctx, sqlc.UpsertImportedRegionParams{Slug: r.Slug, Name: r.Name, DisplayOrder: &order,
		CenterLat: r.CenterLat, CenterLng: r.CenterLng})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// region_iatas references iata_codes, so members Beacon hasn't heard yet are created first, as Seed does.
	if err := s.q.AddIATAs(ctx, r.IATAs); err != nil {
		return false, err
	}
	return true, s.SetRegionIATAs(ctx, id, r.IATAs)
}

func (s *Store) PruneImportedRegions(ctx context.Context, keep []string) ([]string, error) {
	if keep == nil {
		keep = []string{}
	}
	return s.q.PruneImportedRegions(ctx, keep)
}

func (s *Store) ListChannelCatalogues(ctx context.Context) ([]meshmapper.Catalogue, error) {
	rows, err := s.q.ListChannelCatalogues(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]meshmapper.Catalogue, 0, len(rows))
	for _, row := range rows {
		c := meshmapper.Catalogue{IATA: row.Iata, URL: row.Url, Cache: meshmapper.Cache{Payload: row.Payload, CheckedAt: row.CheckedAt.Time,
			AttemptedAt: row.AttemptedAt.Time, NextAttempt: row.NextAttempt.Time, LastError: row.LastError}}
		if row.Etag != nil {
			c.ETag = *row.Etag
		}
		out = append(out, c)
	}
	return out, nil
}

// SaveChannelCatalogue records a fetch; a new payload also replaces the IATA's
// channel members, which are written first so a failure never outlives its snapshot.
func (s *Store) SaveChannelCatalogue(ctx context.Context, iata, url string, cache meshmapper.Cache, fingerprints [][]byte) error {
	if cache.Payload != nil {
		if err := s.SetChannelMembers(ctx, iata, fingerprints); err != nil {
			return err
		}
	}
	params := sqlc.SaveChannelCatalogueParams{
		Iata: iata, Url: url, Payload: cache.Payload, LastError: cache.LastError,
		CheckedAt:   pgtype.Timestamptz{Time: cache.CheckedAt, Valid: !cache.CheckedAt.IsZero()},
		AttemptedAt: pgtype.Timestamptz{Time: cache.AttemptedAt, Valid: true},
		NextAttempt: pgtype.Timestamptz{Time: cache.NextAttempt, Valid: true},
	}
	if !cache.CheckedAt.IsZero() {
		params.Etag = &cache.ETag
	}
	return s.q.SaveChannelCatalogue(ctx, params)
}

// SetChannelMembers makes fingerprints the IATA's exact MeshMapper channel list.
func (s *Store) SetChannelMembers(ctx context.Context, iata string, fingerprints [][]byte) error {
	if fingerprints == nil {
		fingerprints = [][]byte{}
	}
	if err := s.q.DeleteChannelMembersNotIn(ctx, sqlc.DeleteChannelMembersNotInParams{Iata: iata, Keep: fingerprints}); err != nil {
		return err
	}
	return s.q.AddChannelMembers(ctx, sqlc.AddChannelMembersParams{Iata: iata, Fingerprints: fingerprints})
}

func (s *Store) ClearChannelMembers(ctx context.Context) error {
	return s.q.DeleteAllChannelMembers(ctx)
}

func (s *Store) ListIATADetails(ctx context.Context) ([]meshmapper.IATADetails, error) {
	rows, err := s.q.ListIATAs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]meshmapper.IATADetails, 0, len(rows))
	for _, row := range rows {
		d := meshmapper.IATADetails{IATA: row.Iata, Lat: row.ApproxLat, Lng: row.ApproxLng}
		if row.DisplayName != nil {
			d.Name = *row.DisplayName
		}
		out = append(out, d)
	}
	return out, nil
}
