// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"errors"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
)

// LoadState unmarshals the background_state value under key into v. It returns false, nil when
// the key has no row.
func (s *Store) LoadState(ctx context.Context, key string, v any) (bool, error) {
	raw, err := s.q.GetBackgroundState(ctx, key)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return false, err
	}
	return true, nil
}

// SaveState stores v as JSON under key.
func (s *Store) SaveState(ctx context.Context, key string, v any) error {
	return saveState(ctx, s.q, key, v)
}

// saveState marshals v and upserts it through q, which may be bound to a transaction.
func saveState(ctx context.Context, q sqlc.Querier, key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return q.UpsertBackgroundState(ctx, sqlc.UpsertBackgroundStateParams{Key: key, Value: raw})
}
