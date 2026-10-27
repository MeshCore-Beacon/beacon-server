// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) UpsertNode(ctx context.Context, n ingest.UpsertNodeParams, radio ingest.RadioSettings) (uuid.UUID, error) {
	// advert.Timestamp is only meaningful when the advert actually decoded (AdvertTimestamp
	// is left at its zero value otherwise); a zero epoch timestamp would misreport as ~55
	// years of drift, so only compute/store a delta when it's non-zero.
	var driftSeconds *int64
	if n.AdvertTimestamp != 0 {
		d := int64(n.AdvertTimestamp) - time.Now().Unix()
		driftSeconds = &d
	}
	params := sqlc.UpsertNodeParams{
		PublicKey:               n.PublicKey,
		NodeType:                int16(n.NodeType),
		Name:                    &n.Name,
		Latitude:                n.Latitude,
		Longitude:               n.Longitude,
		ClearLocation:           n.ClearLocation,
		DeviceClockDriftSeconds: driftSeconds,
	}
	if radio.FreqMHz != 0 {
		params.RadioFreqMhz = &radio.FreqMHz
		params.RadioSf = &radio.SF
		params.RadioBwKhz = &radio.BWKHz
	}
	row, err := s.q.UpsertNode(ctx, params)
	if err != nil {
		return uuid.Nil, err
	}
	return row.ID, nil
}

func (s *Store) UpsertNodeIATA(ctx context.Context, nodeID uuid.UUID, iata string) error {
	params := sqlc.UpsertNodeIATAParams{NodeID: nodeID, Iata: iata}
	return s.q.UpsertNodeIATA(ctx, params)
}

func (s *Store) UpsertNodeShortID(ctx context.Context, nodeID uuid.UUID, iata string, prefix4 []byte) error {
	return s.q.UpsertNodeShortID(ctx, sqlc.UpsertNodeShortIDParams{
		NodeID:  nodeID,
		Iata:    iata,
		Prefix4: prefix4,
	})
}

func (s *Store) UpsertNodeNeighbor(ctx context.Context, nodeID, neighborID uuid.UUID, iata string, snr *float32, regionScope *string) error {
	return s.q.UpsertNodeNeighbor(ctx, sqlc.UpsertNodeNeighborParams{
		NodeID:      nodeID,
		NeighborID:  neighborID,
		Iata:        iata,
		Snr:         snr,
		RegionScope: regionScope,
	})
}

func (s *Store) SetNodeCapability(ctx context.Context, nodeID uuid.UUID, paths, traces bool) error {
	var errs []error
	if paths {
		errs = append(errs, s.q.SetNodeMultibytePaths(ctx, nodeID))
	}
	if traces {
		errs = append(errs, s.q.SetNodeMultibyteTraces(ctx, nodeID))
	}
	return errors.Join(errs...)
}

func (s *Store) SetNodeDefaultScope(ctx context.Context, nodeID uuid.UUID, scopeID int32) error {
	return s.q.SetNodeDefaultScope(ctx, sqlc.SetNodeDefaultScopeParams{
		ID:             nodeID,
		DefaultScopeID: &scopeID,
	})
}

// uuidKeysetCursor maps the cursors of a (last_seen DESC, id DESC) list to the
// statement's row-cursor pair. A pageCursor (at, id) wins. A numeric cursor
// pairs its millisecond with the nil uuid, which sorts below every id, so the
// row comparison keeps today's "last_seen < ms". Page 1 starts at infinity.
func uuidKeysetCursor(cursor int64, at *time.Time, id uuid.UUID) (pgtype.Timestamptz, uuid.UUID) {
	switch {
	case at != nil:
		return pgtype.Timestamptz{Time: *at, Valid: true}, id
	case cursor > 0:
		return pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}, uuid.Nil
	default:
		return pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true}, uuid.Nil
	}
}

// listNodesParams builds the statement's arguments for one ListNodes request.
func listNodesParams(nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, pubkeyPrefix, name, scope string, cursor int64, pageCursor *api.NodeCursor, limit int32, includeNeighbors bool) sqlc.ListNodesParams {
	var (
		at *time.Time
		id uuid.UUID
	)
	if pageCursor != nil {
		at, id = &pageCursor.LastSeen, pageCursor.ID
	}
	cursorTS, cursorID := uuidKeysetCursor(cursor, at, id)
	return sqlc.ListNodesParams{
		Column1:  nodeType,
		Column2:  iatas,
		Column3:  tristate(supportsMultibytePaths),
		Column4:  tristate(supportsMultibyteTraces),
		Column5:  pubkey,
		Column6:  name,
		Column7:  cursorTS,
		Limit:    limit + 1,
		Column9:  scope,
		Column10: includeNeighbors,
		Column11: pubkeyPrefix,
		Column12: cursorID,
	}
}

func (s *Store) ListNodes(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, pubkeyPrefix, name, scope string, cursor int64, pageCursor *api.NodeCursor, limit int32, includeNeighbors bool) (api.NodePage, error) {
	rows, err := s.q.ListNodes(ctx, listNodesParams(nodeType, iatas, supportsMultibytePaths, supportsMultibyteTraces, pubkey, pubkeyPrefix, name, scope, cursor, pageCursor, limit, includeNeighbors))
	if err != nil {
		return api.NodePage{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.NodeSummary, 0, len(rows))
	for _, v := range rows {
		items = append(items, s.nodeSummary(v))
	}
	page := api.NodePage{Page: api.Page[api.NodeSummary]{Items: items, HasMore: hasMore}}
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		ms := last.LastSeen.Time.UnixMilli()
		next := api.NodeCursor{LastSeen: last.LastSeen.Time, ID: last.ID}.String()
		page.NextCursor = &ms
		page.NextPageCursor = &next
	}
	return page, nil
}

func (s *Store) ListNodesSorted(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, pubkeyPrefix, name, scope string, sort api.NodeSort, pageCursor *api.NodeSortCursor, limit int32, includeNeighbors bool) (api.NodePage, error) {
	params := sqlc.ListNodesSortedParams{
		IncludeNeighbors: includeNeighbors,
		Descending:       sort.Descending,
		Sort:             sort.By,
		NodeType:         nodeType,
		Iatas:            iatas,
		MultibytePaths:   tristate(supportsMultibytePaths),
		MultibyteTraces:  tristate(supportsMultibyteTraces),
		Pubkey:           pubkey,
		Name:             name,
		Scope:            scope,
		PubkeyPrefix:     pubkeyPrefix,
		RowLimit:         limit + 1,
	}
	if pageCursor != nil {
		params.HasCursor = true
		params.CursorNull, params.CursorText, params.CursorNum = pageCursor.Null, pageCursor.Text, pageCursor.Num
		params.CursorSeen, params.CursorID = pageCursor.Seen, pageCursor.ID
	}
	rows, err := s.q.ListNodesSorted(ctx, params)
	if err != nil {
		return api.NodePage{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.NodeSummary, 0, len(rows))
	for _, v := range rows {
		items = append(items, s.nodeSummary(sqlc.ListNodesRow{
			ID: v.ID, PublicKey: v.PublicKey, NodeType: v.NodeType, Name: v.Name, Latitude: v.Latitude,
			Longitude: v.Longitude, LastSeen: v.LastSeen, RadioFreqMhz: v.RadioFreqMhz, RadioSf: v.RadioSf,
			RadioBwKhz: v.RadioBwKhz, DefaultScopeName: v.DefaultScopeName, Iatas: v.Iatas, IsObserver: v.IsObserver,
			ObserverID: v.ObserverID, KnownNeighborCount: v.KnownNeighborCount, NeighborIds: v.NeighborIds,
		}))
	}
	page := api.NodePage{Page: api.Page[api.NodeSummary]{Items: items, HasMore: hasMore}}
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		next := api.NodeSortCursor{Sort: sort, Null: last.KNull, Text: last.KText, Num: last.KNum, Seen: last.KSeen, ID: last.ID}.String()
		page.NextPageCursor = &next
	}
	return page, nil
}

// nodeSummary maps one node list row.
func (s *Store) nodeSummary(v sqlc.ListNodesRow) api.NodeSummary {
	node := api.NodeSummary{
		ID:                 v.ID,
		PublicKey:          hex.EncodeToString(v.PublicKey),
		NodeType:           v.NodeType,
		NodeTypeName:       api.NodeTypeName(v.NodeType),
		Name:               v.Name,
		Latitude:           v.Latitude,
		Longitude:          v.Longitude,
		IsObserver:         v.IsObserver,
		ObserverID:         nullableUUID(v.ObserverID),
		KnownNeighborCount: v.KnownNeighborCount,
		NeighborIDs:        v.NeighborIds,
		Stale:              v.LastSeen.Valid && v.LastSeen.Time.Before(time.Now().Add(-s.staleThreshold)),
	}
	if len(v.Iatas) > 0 {
		if err := json.Unmarshal(v.Iatas, &node.IATAs); err != nil {
			slog.Error("store: failed to unmarshal node iatas", "component", "db", "error", err)
			node.IATAs = []api.NodeIATA{}
		}
	}
	if v.RadioFreqMhz != nil && v.RadioSf != nil && v.RadioBwKhz != nil {
		s := fmt.Sprintf("%g,%g,%d", *v.RadioFreqMhz, *v.RadioBwKhz, *v.RadioSf)
		node.Radio = &s
	}
	return node
}

func (s *Store) GetNode(ctx context.Context, nodeID uuid.UUID) (*api.Node, error) {
	row, err := s.q.GetNodeByID(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	node := &api.Node{
		NodeSummary: api.NodeSummary{
			ID:                 row.ID,
			PublicKey:          hex.EncodeToString(row.PublicKey),
			NodeType:           row.NodeType,
			NodeTypeName:       api.NodeTypeName(row.NodeType),
			Name:               row.Name,
			Latitude:           row.Latitude,
			Longitude:          row.Longitude,
			IsObserver:         row.IsObserver,
			ObserverID:         nullableUUID(row.ObserverID),
			DefaultScope:       row.DefaultScopeName,
			KnownNeighborCount: row.KnownNeighborCount,
			Stale:              row.LastSeen.Valid && row.LastSeen.Time.Before(time.Now().Add(-s.staleThreshold)),
		},
		LocationSource:          row.LocationSource,
		SupportsMultibytePaths:  row.SupportsMultibytePaths,
		SupportsMultibyteTraces: row.SupportsMultibyteTraces,
		MinFirmwareVersion:      row.MinFirmwareVersion,
		FirstSeen:               row.FirstSeen.Time.UnixMilli(),
		LastSeen:                row.LastSeen.Time.UnixMilli(),
		Metadata:                row.Metadata,
	}
	neighbors, err := s.GetNodeNeighbors(ctx, nodeID)
	if err != nil {
		slog.Error(fmt.Sprintf("store: GetNodeNeighbors failed for %s", nodeID), "component", "db", "error", err)
		neighbors = []api.NodeNeighbor{}
	}
	node.Neighbors = neighbors
	if len(row.Iatas) > 0 {
		if err := json.Unmarshal(row.Iatas, &node.IATAs); err != nil {
			slog.Error("store: failed to unmarshal node iatas", "component", "db", "error", err)
			node.IATAs = []api.NodeIATA{}
		}
	}
	if len(row.Meshmapper) > 0 {
		var mm api.MeshMapperRepeater
		if err := json.Unmarshal(row.Meshmapper, &mm); err != nil {
			slog.Error("store: failed to unmarshal MeshMapper repeater", "component", "db", "error", err)
		} else {
			node.MeshMapper = &mm
		}
	}
	if row.RadioFreqMhz != nil && row.RadioSf != nil && row.RadioBwKhz != nil {
		s := fmt.Sprintf("%g,%g,%d", *row.RadioFreqMhz, *row.RadioBwKhz, *row.RadioSf)
		node.Radio = &s
	}
	if row.LastAdvertAt.Valid {
		ms := row.LastAdvertAt.Time.UnixMilli()
		node.LastAdvertAt = &ms
	}
	// Only repeaters (2) and room servers (3) sign adverts with a device clock worth
	// checking; omit entirely (not just zero) for other node types or an unmeasured node
	// per the API contract, so the frontend can distinguish "unknown" from "in sync".
	if (row.NodeType == 2 || row.NodeType == 3) && row.DeviceClockDriftSeconds != nil && row.LastAdvertAt.Valid {
		drift := int(*row.DeviceClockDriftSeconds)
		node.ClockDriftSeconds = &drift
		checkedAt := row.LastAdvertAt.Time.UnixMilli()
		node.ClockCheckedAt = &checkedAt
		outOfSync := time.Duration(abs(*row.DeviceClockDriftSeconds))*time.Second > s.clockDriftThreshold
		node.ClockOutOfSync = &outOfSync
	}
	return node, nil
}

// abs returns the absolute value of a drift. A drift comes from a uint32 timestamp minus now, so it is far inside int64
// and its negation never overflows.
func abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

func (s *Store) GetNodesByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error) {
	rows, err := s.q.GetNodesByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	result := make(map[uuid.UUID]*api.ResolvedNode, len(rows))
	for _, r := range rows {
		result[r.ID] = &api.ResolvedNode{
			ID:        r.ID,
			Name:      r.Name,
			PublicKey: hex.EncodeToString(r.PublicKey),
			Latitude:  r.Latitude,
			Longitude: r.Longitude,
		}
	}
	return result, nil
}

func (s *Store) GetNodeByPubkey(ctx context.Context, pubkey []byte) (uuid.UUID, error) {
	return s.q.GetNodeByPubkey(ctx, pubkey)
}

func (s *Store) GetNodeNeighbors(ctx context.Context, nodeID uuid.UUID) ([]api.NodeNeighbor, error) {
	rows, err := s.q.GetNodeNeighbors(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	seen := make(map[uuid.UUID]int)
	items := make([]api.NodeNeighbor, 0, len(rows))
	for _, r := range rows {
		if idx, ok := seen[r.ID]; ok {
			items[idx].ObservationCount += r.ObservationCount
			if r.LastSeen.Time.After(time.UnixMilli(items[idx].LastSeen)) {
				items[idx].LastSeen = r.LastSeen.Time.UnixMilli()
				items[idx].IATA = r.Iata
				items[idx].SNR = r.Snr
			}
			if r.FirstSeen.Time.Before(time.UnixMilli(items[idx].FirstSeen)) {
				items[idx].FirstSeen = r.FirstSeen.Time.UnixMilli()
			}
			continue
		}
		seen[r.ID] = len(items)
		items = append(items, api.NodeNeighbor{
			ID:               r.ID,
			Name:             r.Name,
			PublicKey:        hex.EncodeToString(r.PublicKey),
			NodeType:         r.NodeType,
			NodeTypeName:     api.NodeTypeName(r.NodeType),
			Latitude:         r.Latitude,
			Longitude:        r.Longitude,
			IATA:             r.Iata,
			ObservationCount: r.ObservationCount,
			FirstSeen:        r.FirstSeen.Time.UnixMilli(),
			LastSeen:         r.LastSeen.Time.UnixMilli(),
			SNR:              r.Snr,
		})
	}
	return items, nil
}

func (s *Store) ReconfirmNeighbors(ctx context.Context) error {
	return s.q.ReconfirmNeighbors(ctx)
}

// DeleteOldNodes deletes nodes not seen since the given cutoff. See the DeleteOldNodes SQL
// query for the observer_owners exclusion and known_routes caveat.
func (s *Store) DeleteOldNodes(ctx context.Context, cutoff time.Time) error {
	return s.q.DeleteOldNodes(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true})
}
