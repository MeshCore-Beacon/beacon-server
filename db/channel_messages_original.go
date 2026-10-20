// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

// OriginalChannelMessages is a channel message page from the original statements, which read every observation:
// the oracle the channel read model is verified against. Run it on a maintenance or direct store; the API router
// refuses the per-channel statement.
func (s *Store) OriginalChannelMessages(ctx context.Context, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	return listChannelMessagesLegacy(ctx, s.q, channelID, since, limit, iatas, scope, cursor)
}
