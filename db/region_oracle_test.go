// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// listPacketsByIATAs is the original region page, from the statement served before the site read model
// (ListPacketsByIATAs, which no page runs now): tests compare region pages with it.
func (s *Store) listPacketsByIATAs(ctx context.Context, payloadTypes, routeTypes []int16, iatas []string, scopes []string, since, until time.Time, cursor int64, limit int32) (api.Page[api.PacketSummary], error) {
	var cursorTS pgtype.Timestamptz
	if cursor > 0 {
		cursorTS = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	var sinceTS pgtype.Timestamptz
	if !since.IsZero() {
		sinceTS = pgtype.Timestamptz{Time: since, Valid: true}
	}
	var untilTS pgtype.Timestamptz
	if !until.IsZero() {
		untilTS = pgtype.Timestamptz{Time: until, Valid: true}
	}
	// A packet appears once per observer that heard it at a site
	// ((packet_hash, observer_id) is unique), so scan deep enough per site
	// to still fill the page after collapsing those duplicates.
	rows, err := s.q.ListPacketsByIATAs(ctx, sqlc.ListPacketsByIATAsParams{
		Iatas:        iatas,
		ScanDepth:    (limit + 1) * 8,
		CursorTs:     cursorTS,
		PayloadTypes: payloadTypes,
		RouteTypes:   routeTypes,
		SinceTs:      sinceTS,
		UntilTs:      untilTS,
		ScopeNames:   scopes,
		PageLimit:    limit + 1,
	})
	if err != nil {
		return api.Page[api.PacketSummary]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	// Observers duplicate a packet across the scan, so the page can collapse
	// short while the site still has history below scan_floor. Stopping here
	// would strand every older packet behind a hasMore=false.
	var scanFloor pgtype.Timestamptz
	if len(rows) > 0 && rows[0].ScanSaturated {
		hasMore = true
		scanFloor = rows[0].ScanFloor
	}
	items := make([]api.PacketSummary, 0, len(rows))
	lookups := make([]endpointLookup, 0, len(rows))
	for _, v := range rows {
		var lookup endpointLookup
		item := api.PacketSummary{
			PacketHash:       hex.EncodeToString(v.PacketHash),
			PayloadType:      v.PayloadType,
			PayloadTypeName:  api.PayloadTypeName(v.PayloadType),
			RouteType:        v.RouteType,
			RouteTypeName:    api.RouteTypeName(v.RouteType),
			Scope:            v.ScopeName,
			FirstHeardAt:     v.FirstHeardAt.Time.UnixMilli(),
			LastHeardAt:      v.LastHeardAt.Time.UnixMilli(),
			ObservationCount: int32(v.ObservationCount),
		}
		if v.Summary != "" {
			item.Summary = &v.Summary
		}
		if v.LatestObserverID != (uuid.UUID{}) {
			item.LatestObserver = &api.PacketLatestObserver{
				ID:          v.LatestObserverID,
				DisplayName: v.LatestObserverName,
				IATA:        v.LatestObserverIata,
			}
			lookup = endpointLookup{iata: v.LatestObserverIata, ep: parsePacketEndpoints(v.PayloadType, v.RawPayload, v.OriginPubkey)}
			item.LatestObserver.PathLength, item.LatestObserver.PathBytes = buildLatestObserverPath(
				&v.LatestObserverPathLengthByte, &v.LatestObserverHashSize, &v.LatestObserverHopCount, v.LatestObserverPathBytes,
			)
		}
		items = append(items, item)
		lookups = append(lookups, lookup)
	}
	s.fillLatestEndpoints(ctx, items, lookups)
	// Cursor follows site-local recency, not the packet's global last_heard_at.
	var nextCursor *int64
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1].SiteHeardAt.Time
		// A saturated scan never read below its floor. Paging past it would
		// skip that band; resuming at it only repeats packets already shown.
		if scanFloor.Valid && scanFloor.Time.After(last) {
			last = scanFloor.Time
		}
		ms := last.UnixMilli()
		nextCursor = &ms
	}
	return api.Page[api.PacketSummary]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}
