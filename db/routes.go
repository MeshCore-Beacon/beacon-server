package db

import (
	"context"
	"encoding/hex"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

func (s *Store) UpsertKnownRoute(ctx context.Context, nodeIDs []uuid.UUID, hashPrefix [][]byte, iata string, hopCount int32) error {
	return s.q.UpsertKnownRoute(ctx, sqlc.UpsertKnownRouteParams{
		NodeIds:    nodeIDs,
		HashPrefix: hashPrefix,
		Iata:       iata,
		HopCount:   int32(hopCount),
	})
}

func (s *Store) ListKnownRoutes(ctx context.Context, iata string, hopCount int32, cursor int64, limit int32) ([]api.KnownRoute, error) {
	rows, err := s.q.ListKnownRoutes(ctx, sqlc.ListKnownRoutesParams{
		Column1: iata,
		Column2: hopCount,
		Column3: cursor,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	return toKnownRoutes(rows), nil
}

func (s *Store) SearchKnownRoutes(ctx context.Context, iata, fromHash, toHash string) ([]api.KnownRoute, error) {
	fromBytes, err := hex.DecodeString(fromHash)
	if err != nil {
		return nil, err
	}
	toBytes, err := hex.DecodeString(toHash)
	if err != nil {
		return nil, err
	}
	rows, err := s.q.SearchKnownRoutes(ctx, sqlc.SearchKnownRoutesParams{
		Iata:    iata,
		Column2: fromBytes,
		Column3: toBytes,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.KnownRoute, 0, len(rows))
	for _, r := range rows {
		// find positions and slice to the subsequence
		fromPos, toPos := -1, -1
		for i, h := range r.HashPrefix {
			if fromPos == -1 && hex.EncodeToString(h) == fromHash {
				fromPos = i
			}
			if fromPos != -1 && hex.EncodeToString(h) == toHash {
				toPos = i
				break
			}
		}
		if fromPos == -1 || toPos == -1 {
			continue
		}
		nodeIDs := r.NodeIds[fromPos : toPos+1]
		hashPrefix := r.HashPrefix[fromPos : toPos+1]
		hops := make([]api.RouteHop, 0, len(nodeIDs))
		for i, nodeID := range nodeIDs {
			hop := api.RouteHop{NodeID: nodeID}
			if i < len(hashPrefix) {
				hop.HashBytes = hex.EncodeToString(hashPrefix[i])
			}
			hops = append(hops, hop)
		}
		items = append(items, api.KnownRoute{
			ID:        r.ID,
			IATA:      r.Iata,
			HopCount:  int32(len(hops)),
			Hops:      hops,
			FirstSeen: r.FirstSeen.Time.UnixMilli(),
			LastSeen:  r.LastSeen.Time.UnixMilli(),
		})
	}
	return items, nil
}

func toKnownRoutes(rows []sqlc.KnownRoute) []api.KnownRoute {
	items := make([]api.KnownRoute, 0, len(rows))
	for _, r := range rows {
		hops := make([]api.RouteHop, 0, len(r.NodeIds))
		for i, nodeID := range r.NodeIds {
			hop := api.RouteHop{
				NodeID: nodeID,
			}
			if i < len(r.HashPrefix) {
				hop.HashBytes = hex.EncodeToString(r.HashPrefix[i])
			}
			hops = append(hops, hop)
		}
		items = append(items, api.KnownRoute{
			ID:        r.ID,
			IATA:      r.Iata,
			HopCount:  r.HopCount,
			Hops:      hops,
			FirstSeen: r.FirstSeen.Time.UnixMilli(),
			LastSeen:  r.LastSeen.Time.UnixMilli(),
		})
	}
	return items
}
