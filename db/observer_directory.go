// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (s *Store) ListObserverDirectory(ctx context.Context, query api.ObserverDirectoryQuery) (*api.ObserverDirectory, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var id uuid.UUID
	var err error
	if query.Snapshot != "" {
		id, err = uuid.Parse(query.Snapshot)
	} else {
		id, err = s.observerDirectorySnapshot(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	raw, err := s.q.GetObserverDirectoryPage(ctx, sqlc.GetObserverDirectoryPageParams{ID: id, Column1: query.Cursor, Column2: query.Limit})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, api.ErrDirectoryExpired
	}
	if err != nil {
		return nil, err
	}
	var page api.ObserverDirectory
	if err := json.Unmarshal([]byte(raw), &page); err != nil {
		return nil, err
	}
	return &page, nil
}

func (s *Store) observerDirectorySnapshot(ctx context.Context, query api.ObserverDirectoryQuery) (uuid.UUID, error) {
	data, err := json.Marshal(query)
	if err != nil {
		return uuid.Nil, err
	}
	key := sha256.Sum256(data)
	id, err := s.q.FindObserverDirectorySnapshot(ctx, key[:])
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer tx.Rollback(context.Background())
	q := sqlc.New(tx)
	locked, err := q.LockObserverDirectoryCreation(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	if !locked {
		return uuid.Nil, api.ErrDirectoryBusy
	}
	if err := q.PruneObserverDirectorySnapshots(ctx); err != nil {
		return uuid.Nil, err
	}
	id, err = q.FindObserverDirectorySnapshot(ctx, key[:])
	if errors.Is(err, pgx.ErrNoRows) {
		available, err := q.ObserverDirectoryHasCapacity(ctx)
		if err != nil {
			return uuid.Nil, err
		}
		if !available {
			return uuid.Nil, api.ErrDirectoryBusy
		}
		id, err = q.CreateObserverDirectorySnapshot(ctx, sqlc.CreateObserverDirectorySnapshotParams{
			Column12: query.MatchNone, Column1: ts(time.UnixMilli(query.Since)), Column2: ts(time.UnixMilli(query.Until)), Column3: query.IATAs,
			Column4: query.Broker, Column5: query.Name, Column6: query.Status, Column7: query.Scope, Column8: query.Type, Column9: query.Sort,
			ID: uuid.New(), QueryKey: key[:],
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23514" {
				return uuid.Nil, api.ErrDirectoryBusy
			}
			return uuid.Nil, err
		}
	} else if err != nil {
		return uuid.Nil, err
	}
	withinBudget, err := q.ObserverDirectoryWithinBudget(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	if !withinBudget {
		return uuid.Nil, api.ErrDirectoryBusy
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}
