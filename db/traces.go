// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: agpl

package db

import (
	"context"
	"encoding/hex"
	"strings"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) ListTraceTags(ctx context.Context, iatas []string, scope string, since, until time.Time, cursor time.Time, limit int32) ([]api.TraceTagSummary, error) {
	iataFilter := strings.Join(iatas, ",")
	var sinceTS, untilTS, cursorTS pgtype.Timestamptz
	if !since.IsZero() {
		sinceTS = pgtype.Timestamptz{Time: since, Valid: true}
	}
	if !until.IsZero() {
		untilTS = pgtype.Timestamptz{Time: until, Valid: true}
	}
	if !cursor.IsZero() {
		cursorTS = pgtype.Timestamptz{Time: cursor, Valid: true}
	}
	rows, err := s.q.ListTraceTags(ctx, sqlc.ListTraceTagsParams{
		Column1: iataFilter,
		Column2: scope,
		Column3: sinceTS,
		Column4: untilTS,
		Column5: cursorTS,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.TraceTagSummary, 0, len(rows))
	for _, r := range rows {
		items = append(items, api.TraceTagSummary{
			TraceTag:     r.TraceTag,
			FirstHeardAt: r.FirstHeardAt.Time.UnixMilli(),
			LastHeardAt:  r.LastHeardAt.Time.UnixMilli(),
			PacketCount:  r.PacketCount,
			IATACount:    r.IataCount,
		})
	}
	return items, nil
}

func (s *Store) GetTraceByTag(ctx context.Context, tag string) (*api.TraceDetail, error) {
	rows, err := s.q.GetPacketsByTraceTag(ctx, tag)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	detail := &api.TraceDetail{
		TraceTag: tag,
		Packets:  make([]api.TracePacket, 0, len(rows)),
	}
	for _, r := range rows {
		packet := api.TracePacket{
			PacketHash:    r.PacketHashHex,
			RouteType:     r.RouteType,
			RouteTypeName: api.RouteTypeName(r.RouteType),
			Scope:         r.ScopeName,
			FirstHeardAt:  r.FirstHeardAt.Time.UnixMilli(),
			LastHeardAt:   r.LastHeardAt.Time.UnixMilli(),
		}
		// fetch observations to get IATAs for route resolution
		packetHashBytes, err := hex.DecodeString(r.PacketHashHex)
		if err == nil {
			obsRows, err := s.q.ListObservationsForPacket(ctx, packetHashBytes)
			if err == nil && len(obsRows) > 0 {
				iatas := make([]string, 0, len(obsRows))
				seen := make(map[string]struct{})
				for _, v := range obsRows {
					if _, ok := seen[v.Iata]; !ok {
						seen[v.Iata] = struct{}{}
						iatas = append(iatas, v.Iata)
					}
				}
				packet.ResolvedRoute = s.resolveTraceRoute(ctx, r.ParsedPayload, iatas)
			}
		}
		detail.Packets = append(detail.Packets, packet)
	}
	return detail, nil
}
