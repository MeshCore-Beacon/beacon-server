// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"errors"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

const foreignKeyViolation = "23503"

// UserAtlas reads the user's Atlas with ids resolved now. A user with none reads as revision 0, 24h, no pins.
func (s *Store) UserAtlas(ctx context.Context, userID uuid.UUID) (api.Atlas, error) {
	return readUserAtlas(ctx, s.q, userID)
}

func readUserAtlas(ctx context.Context, q sqlc.Querier, userID uuid.UUID) (api.Atlas, error) {
	rng, rev := "24h", int64(0)
	rows, err := q.ReadUserAtlas(ctx, userID)
	if err != nil {
		return api.Atlas{}, err
	}
	var stored []api.StoredPin
	var keys [][]byte
	for _, r := range rows {
		rng, rev = r.Range, r.Revision
		if r.Kind == nil { // the header row of an Atlas with no pins
			continue
		}
		p := api.StoredPin{Kind: *r.Kind, Key: r.PublicKey, Name: *r.Name, Tiles: r.Tiles}
		if r.View != nil {
			p.View = *r.View
		}
		stored = append(stored, p)
		keys = append(keys, r.PublicKey)
	}
	nodeIDs, observerIDs := map[string]uuid.UUID{}, map[string]uuid.UUID{}
	if len(keys) > 0 {
		ns, err := q.NodeIDsByKeys(ctx, keys)
		if err != nil {
			return api.Atlas{}, err
		}
		for _, n := range ns {
			nodeIDs[hex.EncodeToString(n.PublicKey)] = n.ID
		}
		os, err := q.ObserverIDsByKeys(ctx, keys)
		if err != nil {
			return api.Atlas{}, err
		}
		for _, o := range os {
			observerIDs[hex.EncodeToString(o.PublicKey)] = o.ID
		}
	}
	return api.ResolveAtlas(rng, rev, stored, nodeIDs, observerIDs), nil
}

// ObserverKeysByIDs maps each id that is an observer to its public key.
func (s *Store) ObserverKeysByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]byte, error) {
	out := map[uuid.UUID][]byte{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.q.ObserverKeysByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		out[r.ID] = r.PublicKey
	}
	return out, nil
}

// ReplaceUserAtlas replaces the user's whole Atlas when base is the stored revision and returns the document it
// committed, read in the same transaction under the Atlas lock. ok is false, and nothing changes, when the revision
// differs. A user that no longer exists gives users.ErrNotFound.
func (s *Store) ReplaceUserAtlas(ctx context.Context, userID uuid.UUID, base int64, rng string, pins []api.StoredPin) (doc api.Atlas, ok bool, err error) {
	err = s.pollTx(ctx, func(q *sqlc.Queries) error {
		if err := q.EnsureUserAtlas(ctx, userID); err != nil {
			return err
		}
		cur, err := q.LockUserAtlas(ctx, userID)
		if err != nil {
			return err
		}
		if cur.Revision != base {
			return nil
		}
		if err := q.DeleteUserAtlasPins(ctx, userID); err != nil {
			return err
		}
		for i, p := range pins {
			arg := sqlc.InsertUserAtlasPinParams{UserID: userID, Position: int16(i), Kind: p.Kind, PublicKey: p.Key, Name: p.Name, Tiles: p.Tiles}
			if p.View != "" {
				v := p.View
				arg.View = &v
			}
			if err := q.InsertUserAtlasPin(ctx, arg); err != nil {
				return err
			}
		}
		// A radio taken off the Atlas takes its alert rules with it.
		if err := q.DeleteUnpinnedAlertRules(ctx, userID); err != nil {
			return err
		}
		if _, err := q.BumpUserAtlas(ctx, sqlc.BumpUserAtlasParams{UserID: userID, Range: rng}); err != nil {
			return err
		}
		if doc, err = readUserAtlas(ctx, q, userID); err != nil {
			return err
		}
		ok = true
		return nil
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == foreignKeyViolation {
		return api.Atlas{}, false, users.ErrNotFound
	}
	return doc, ok, err
}
