// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var _ api.AccountStore = (*Store)(nil)

func (s *Store) CreateAccount(ctx context.Context, name string) (api.Account, error) {
	name, err := api.NormalizeAccountName(name)
	if err != nil {
		return api.Account{}, err
	}
	row, err := s.q.CreateAccount(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return api.Account{}, api.ErrAccountNameConflict
	}
	if err != nil {
		return api.Account{}, err
	}
	return accountFromRow(row), nil
}

func (s *Store) ListAccounts(ctx context.Context) ([]api.Account, error) {
	// ponytail: unpaginated operator list; add cursors if account counts grow large.
	rows, err := s.q.ListAccounts(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]api.Account, 0, len(rows))
	for _, row := range rows {
		items = append(items, accountFromRow(row))
	}
	return items, nil
}

func (s *Store) GetAccount(ctx context.Context, id uuid.UUID) (api.Account, error) {
	row, err := s.q.GetAccount(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return api.Account{}, api.ErrAccountNotFound
	}
	if err != nil {
		return api.Account{}, err
	}
	return accountFromRow(row), nil
}

func (s *Store) DeactivateAccount(ctx context.Context, id uuid.UUID) error {
	result, err := s.q.DeactivateAccount(ctx, id)
	if err != nil {
		return err
	}
	if !result.Found {
		return api.ErrAccountNotFound
	}
	if !result.Deactivated {
		return api.ErrAccountInactive
	}
	return nil
}

func accountFromRow(row sqlc.Account) api.Account {
	account := api.Account{ID: row.ID, Name: row.Name, CreatedAt: row.CreatedAt.Time.UTC(), Active: !row.DeactivatedAt.Valid}
	if row.DeactivatedAt.Valid {
		when := row.DeactivatedAt.Time.UTC()
		account.DeactivatedAt = &when
	}
	return account
}
