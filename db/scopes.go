// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: agpl

package db

import (
	"context"
	"strings"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
)

func (s *Store) UpsertTransportScope(ctx context.Context, name, displayName string, transportKey, keyFingerprint []byte) error {
	var dn *string
	if displayName != "" {
		dn = &displayName
	}
	return s.q.UpsertTransportScope(ctx, sqlc.UpsertTransportScopeParams{
		Name:           name,
		DisplayName:    dn,
		TransportKey:   transportKey,
		KeyFingerprint: keyFingerprint,
	})
}

func (s *Store) GetTransportScopes(ctx context.Context) ([]scopestore.Entry, error) {
	rows, err := s.q.GetTransportScopes(ctx)
	if err != nil {
		return nil, err
	}
	entries := make([]scopestore.Entry, 0, len(rows))
	for _, r := range rows {
		entries = append(entries, scopestore.Entry{
			Name:           r.Name,
			TransportKey:   r.TransportKey,
			KeyFingerprint: r.KeyFingerprint,
		})
	}
	return entries, nil
}

func (s *Store) GetTransportScopeByName(ctx context.Context, name string) (int32, error) {
	return s.q.GetTransportScopeByName(ctx, name)
}

// GetScopeNames returns the names of all configured transport scopes.
func (s *Store) GetScopeNames(ctx context.Context) ([]string, error) {
	return s.q.GetScopeNames(ctx)
}

// GetScopesByIATAs returns scope summaries filtered by the given IATA codes.
func (s *Store) GetScopesByIATAs(ctx context.Context, iatas []string) ([]api.ScopeSummary, error) {
	iataFilter := strings.Join(iatas, ",")
	rows, err := s.q.GetScopesByIATAs(ctx, iataFilter)
	if err != nil {
		return nil, err
	}
	items := make([]api.ScopeSummary, 0, len(rows))
	for _, r := range rows {
		items = append(items, api.ScopeSummary{
			Name:          r.Name,
			ObserverCount: r.ObserverCount,
			NodeCount:     r.NodeCount,
			IATACount:     r.IataCount,
		})
	}
	return items, nil
}

// GetScopeByName returns full detail for a single scope by its normalized name.
func (s *Store) GetScopeByName(ctx context.Context, name string) (*api.ScopeDetail, error) {
	row, err := s.q.GetScopeByName(ctx, name)
	if err != nil {
		return nil, err
	}
	return &api.ScopeDetail{
		Name:          row.Name,
		PacketCount:   row.PacketCount,
		ObserverCount: row.ObserverCount,
		NodeCount:     row.NodeCount,
		IATACount:     row.IataCount,
		IATAs:         row.Iatas,
	}, nil
}
