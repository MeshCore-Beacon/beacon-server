// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package db

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"math"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5/pgtype"
)

func savedRoutePath(prefixes [][]byte, count int32) (int16, []byte, bool) {
	if count < 2 || count > 63 || len(prefixes) != int(count) {
		return 0, nil, false
	}
	width := len(prefixes[0])
	if width < 1 || width > 3 {
		return 0, nil, false
	}
	for _, p := range prefixes {
		if len(p) != width {
			return 0, nil, false
		}
	}
	return int16(width), bytes.Join(prefixes, nil), true
}

func (s *Store) GetRouteEvidence(ctx context.Context, iata, key string, query api.RouteEvidenceQuery) (*api.RouteEvidence, error) {
	if !api.ValidRouteEvidenceKey(iata, key) || !api.ValidRouteEvidenceWindow(query.Since, query.Until) || query.Limit < 1 || query.Limit > 200 {
		return nil, api.ErrRouteEvidenceInput
	}
	if c := query.Cursor; c != nil {
		if c.IATA != iata || c.PathKey != key || !c.Since.Equal(query.Since) || !c.Until.Equal(query.Until) || c.ID <= 0 || c.HeardAt.Before(query.Since) || !c.HeardAt.Before(query.Until) {
			return nil, api.ErrRouteEvidenceInput
		}
	}
	keyBytes, _ := hex.DecodeString(key)
	row, err := s.q.GetRouteEvidenceRoute(ctx, sqlc.GetRouteEvidenceRouteParams{Iata: iata, PathKey: keyBytes})
	if err != nil {
		return nil, err
	}
	nodes, err := s.GetNodesByIDs(ctx, row.NodeIds)
	if err != nil {
		return nil, err
	}
	route := toKnownRoutes([]knownRouteRow{{ID: row.ID, NodeIds: row.NodeIds, HashPrefix: row.HashPrefix, Iata: row.Iata, HopCount: row.HopCount, FirstSeen: row.FirstSeen, LastSeen: row.LastSeen, ObservationCount: row.ObservationCount}}, nodes)[0]
	route.PathKey = hex.EncodeToString(row.PathKey)
	out := &api.RouteEvidence{Items: []api.RouteObservation{}, Route: route, WindowStart: query.Since.UnixMilli(), WindowEnd: query.Until.UnixMilli(), GeneratedAt: time.Now().UnixMilli(), MatchType: "saved_path_prefixes"}
	width, path, valid := savedRoutePath(row.HashPrefix, row.HopCount)
	if !valid {
		return out, nil
	}
	out.MatchAvailable = true
	out.HashSize = width
	out.PathBytes = hex.EncodeToString(path)
	beforeAt, beforeID := query.Until, int64(math.MaxInt64)
	if query.Cursor != nil {
		beforeAt, beforeID = query.Cursor.HeardAt, query.Cursor.ID
	}
	ts := func(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }
	digest := md5.Sum(path)
	rows, err := s.q.ListRouteEvidence(ctx, sqlc.ListRouteEvidenceParams{Iata: iata, HashSize: width, HopCount: int16(row.HopCount), PathDigest: digest[:], PathBytes: path, Since: ts(query.Since), Until: ts(query.Until), BeforeAt: ts(beforeAt), BeforeID: beforeID, PageLimit: query.Limit + 1})
	if err != nil {
		return nil, err
	}
	out.HasMore = len(rows) > int(query.Limit)
	if out.HasMore {
		rows = rows[:query.Limit]
	}
	for _, r := range rows {
		payload := int16(-1)
		if r.PayloadType != nil {
			payload = *r.PayloadType
		}
		snr := r.Snr
		if snr != nil && (math.IsNaN(float64(*snr)) || math.IsInf(float64(*snr), 0)) {
			snr = nil
		}
		out.Items = append(out.Items, api.RouteObservation{ID: r.ID, PacketHash: hex.EncodeToString(r.PacketHash), ObserverID: r.ObserverID, ObserverName: r.ObserverName, HeardAt: r.HeardAt.Time.UnixMilli(), PayloadType: payload, PayloadTypeName: api.PayloadTypeName(payload), RSSI: r.Rssi, SNR: snr})
	}
	if out.HasMore {
		last := rows[len(rows)-1]
		cursor := (api.RouteEvidenceCursor{IATA: iata, PathKey: key, Since: query.Since, Until: query.Until, HeardAt: last.HeardAt.Time, ID: last.ID}).String()
		out.NextPageCursor = &cursor
	}
	return out, nil
}
