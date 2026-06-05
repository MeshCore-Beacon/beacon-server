package db

import (
	"context"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
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
