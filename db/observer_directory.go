// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

func (s *Store) ListObserverDirectory(ctx context.Context, query api.ObserverDirectoryQuery) (*api.ObserverDirectory, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	raw, err := s.q.ListObserverDirectory(ctx, sqlc.ListObserverDirectoryParams{
		Column1: ts(time.UnixMilli(query.Since)), Column2: ts(time.UnixMilli(query.Until)), Column3: query.IATAs,
		Column4: query.Broker, Column5: query.Name, Column6: query.Status, Column7: query.Scope, Column8: query.Type, Column9: query.Sort,
		Column10: query.Cursor, Column11: query.Limit, Column12: query.MatchNone,
	})
	if err != nil {
		return nil, err
	}
	var page api.ObserverDirectory
	if err := json.Unmarshal([]byte(raw), &page); err != nil {
		return nil, err
	}
	return &page, nil
}
