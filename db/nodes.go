// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) UpsertNode(ctx context.Context, n ingest.UpsertNodeParams, radio ingest.RadioSettings) (uuid.UUID, error) {
	params := sqlc.UpsertNodeParams{
		PublicKey: n.PublicKey,
		NodeType:  int16(n.NodeType),
		Name:      &n.Name,
		Latitude:  n.Latitude,
		Longitude: n.Longitude,
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

func (s *Store) UpsertNodeNeighbor(ctx context.Context, nodeID, neighborID uuid.UUID, iata string) error {
	return s.q.UpsertNodeNeighbor(ctx, sqlc.UpsertNodeNeighborParams{
		NodeID:     nodeID,
		NeighborID: neighborID,
		Iata:       iata,
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

func (s *Store) ListNodes(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, name, scope string, cursor int64, limit int32) (api.Page[api.NodeSummary], error) {
	var cursorTS pgtype.Timestamptz
	if cursor > 0 {
		cursorTS = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	iataFilter := strings.Join(iatas, ",")
	rows, err := s.q.ListNodes(ctx, sqlc.ListNodesParams{
		Column1: nodeType,
		Column2: iataFilter,
		Column3: tristate(supportsMultibytePaths),
		Column4: tristate(supportsMultibyteTraces),
		Column5: pubkey,
		Column6: name,
		Column7: cursorTS,
		Limit:   limit + 1,
		Column9: scope,
	})
	if err != nil {
		return api.Page[api.NodeSummary]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.NodeSummary, 0, len(rows))
	for _, v := range rows {
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
		}
		if len(v.Iatas) > 0 {
			if err := json.Unmarshal(v.Iatas, &node.IATAs); err != nil {
				log.Printf("store: failed to unmarshal node iatas: %v", err)
				node.IATAs = []api.NodeIATA{}
			}
		}
		if v.RadioFreqMhz != nil && v.RadioSf != nil && v.RadioBwKhz != nil {
			s := fmt.Sprintf("%.1f,%g,%d", *v.RadioFreqMhz, *v.RadioBwKhz, *v.RadioSf)
			node.Radio = &s
		}
		items = append(items, node)
	}
	var nextCursor *int64
	if hasMore && len(items) > 0 {
		ms := rows[len(rows)-1].LastSeen.Time.UnixMilli()
		nextCursor = &ms
	}
	return api.Page[api.NodeSummary]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
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
		log.Printf("store: GetNodeNeighbors failed for %s: %v", nodeID, err)
		neighbors = []api.NodeNeighbor{}
	}
	node.Neighbors = neighbors
	if len(row.Iatas) > 0 {
		if err := json.Unmarshal(row.Iatas, &node.IATAs); err != nil {
			log.Printf("store: failed to unmarshal node iatas: %v", err)
			node.IATAs = []api.NodeIATA{}
		}
	}
	if row.RadioFreqMhz != nil && row.RadioSf != nil && row.RadioBwKhz != nil {
		s := fmt.Sprintf("%.1f,%g,%d", *row.RadioFreqMhz, *row.RadioBwKhz, *row.RadioSf)
		node.Radio = &s
	}
	if row.LastAdvertAt.Valid {
		ms := row.LastAdvertAt.Time.UnixMilli()
		node.LastAdvertAt = &ms
	}
	return node, nil
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
		})
	}
	return items, nil
}
