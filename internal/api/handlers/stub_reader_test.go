// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

// stubReader satisfies api.Reader with configurable function fields.
// Unset fields return zero values. Use it for both validation tests
// (leave all fields nil) and happy path tests (set only what you need).
type stubReader struct {
	listIATAs                    func(ctx context.Context) ([]api.IATA, error)
	getIATA                      func(ctx context.Context, iata string) (*api.IATA, error)
	listRegions                  func(ctx context.Context) ([]api.RegionSummary, error)
	getRegion                    func(ctx context.Context, regionID int32) (*api.Region, error)
	getRegionBySlug              func(ctx context.Context, slug string) (*api.Region, error)
	listChannels                 func(ctx context.Context, limit int32, hash []byte, iatas []string, cursor int64) (api.Page[api.ChannelSummary], error)
	getChannel                   func(ctx context.Context, channelID int32) (*api.Channel, error)
	listChannelMessages          func(ctx context.Context, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error)
	listChannelMessagesByHash    func(ctx context.Context, hash []byte, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error)
	listMessagesAfterID          func(ctx context.Context, afterID int64, iatas []string, scope string, limit int32) ([]api.ChannelMessage, error)
	listObservers                func(ctx context.Context, iatas []string, observerType, broker, status, name, scope string, cursor int64, limit int32) (api.Page[api.ObserverSummary], error)
	getObserver                  func(ctx context.Context, observerID uuid.UUID) (*api.Observer, error)
	getObserverTelemetry         func(ctx context.Context, observerID uuid.UUID, since, until time.Time, afterID int64) (*api.ObserverTelemetry, error)
	getObserverTelemetryBucketed func(ctx context.Context, observerID uuid.UUID, since, until time.Time, bucketHours int32) ([]api.ObserverTelemetryPoint, error)
	getObserverScopes            func(ctx context.Context, observerID uuid.UUID) ([]string, error)
	listObserverAdverts          func(ctx context.Context, observerID uuid.UUID, cursor int64, limit int32) (api.Page[api.AdvertObservation], error)
	listNodes                    func(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, pubkeyPrefix, name, scope string, cursor int64, limit int32, includeNeighbors bool) (api.Page[api.NodeSummary], error)
	getNode                      func(ctx context.Context, nodeID uuid.UUID) (*api.Node, error)
	getNodeNeighbors             func(ctx context.Context, nodeID uuid.UUID) ([]api.NodeNeighbor, error)
	listNodeObservations         func(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (api.Page[api.PacketObservationSummary], error)
	listPackets                  func(ctx context.Context, payloadTypes, routeTypes []int16, iatas []string, scopes []string, since, until time.Time, cursor int64, limit int32) (api.Page[api.PacketSummary], error)
	listPacketsAfterID           func(ctx context.Context, afterObservationID int64, payloadType, routeType int16, iatas []string, scope string, limit int32) ([]api.PacketSummary, error)
	getPacket                    func(ctx context.Context, packetHash []byte) (*api.Packet, error)
	getRadioPresets              func(ctx context.Context, preset string, iatas []string) ([]api.RadioPreset, error)
	getStatsOverview             func(ctx context.Context, iatas []string) (*api.StatsOverview, error)
	getStatsObservations         func(ctx context.Context, iatas []string, since time.Time) ([]api.ObservationPoint, error)
	getStatsPayloadBreakdown     func(ctx context.Context, iatas []string, since time.Time) ([]api.PayloadBreakdownItem, error)
	getStatsTopNodes             func(ctx context.Context, iatas []string, limit int32) ([]api.TopNode, error)
	getStatsTopObservers         func(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopObserver, error)
	getStatsTopAdvertisers       func(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopAdvertiser, error)
	getStatsTopTalkers           func(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopTalker, error)
	getScopeStats                func(ctx context.Context) ([]api.ScopeStats, error)
	getStatsNodeTypes            func(ctx context.Context, iatas []string) ([]api.NodeTypeCount, error)
	getScopeNames                func(ctx context.Context) ([]string, error)
	getScopesByIATAs             func(ctx context.Context, iatas []string) ([]api.ScopeSummary, error)
	getScopeByName               func(ctx context.Context, name string) (*api.ScopeDetail, error)
	listTraceTags                func(ctx context.Context, iatas []string, scope, traceType string, since, until time.Time, cursor time.Time, limit int32) ([]api.TraceTagSummary, error)
	getTraceByTag                func(ctx context.Context, tag string) (*api.TraceDetail, error)
	listKnownRoutes              func(ctx context.Context, iata string, hopCount int32, cursor time.Time, limit int32) ([]api.KnownRoute, error)
	searchKnownRoutes            func(ctx context.Context, iata, fromHash, toHash string) ([]api.KnownRoute, error)
	getKnownRoutesByNode         func(ctx context.Context, iata string, nodeID uuid.UUID) ([]api.KnownRoute, error)
	getCrossIATANeighbors        func(ctx context.Context, nodeID uuid.UUID, iata string) ([]api.NodeNeighbor, error)
	searchCrossIATARoutes        func(ctx context.Context, fromHash, fromIATA, toHash, toIATA string) ([]api.CrossIATARoute, error)
	getNodesByIDs                func(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error)
}

func (s stubReader) ListIATAs(ctx context.Context) ([]api.IATA, error) {
	if s.listIATAs != nil {
		return s.listIATAs(ctx)
	}
	return nil, nil
}

func (s stubReader) GetIATA(ctx context.Context, iata string) (*api.IATA, error) {
	if s.getIATA != nil {
		return s.getIATA(ctx, iata)
	}
	return nil, nil
}

func (s stubReader) ListRegions(ctx context.Context) ([]api.RegionSummary, error) {
	if s.listRegions != nil {
		return s.listRegions(ctx)
	}
	return nil, nil
}

func (s stubReader) GetRegion(ctx context.Context, regionID int32) (*api.Region, error) {
	if s.getRegion != nil {
		return s.getRegion(ctx, regionID)
	}
	return nil, nil
}

func (s stubReader) GetRegionBySlug(ctx context.Context, slug string) (*api.Region, error) {
	if s.getRegionBySlug != nil {
		return s.getRegionBySlug(ctx, slug)
	}
	return nil, nil
}

func (s stubReader) ListChannels(ctx context.Context, limit int32, hash []byte, iatas []string, cursor int64) (api.Page[api.ChannelSummary], error) {
	if s.listChannels != nil {
		return s.listChannels(ctx, limit, hash, iatas, cursor)
	}
	return api.Page[api.ChannelSummary]{}, nil
}

func (s stubReader) GetChannel(ctx context.Context, channelID int32) (*api.Channel, error) {
	if s.getChannel != nil {
		return s.getChannel(ctx, channelID)
	}
	return nil, nil
}

func (s stubReader) ListChannelMessages(ctx context.Context, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	if s.listChannelMessages != nil {
		return s.listChannelMessages(ctx, channelID, since, limit, iatas, scope, cursor)
	}
	return api.Page[api.ChannelMessage]{}, nil
}

func (s stubReader) ListChannelMessagesByHash(ctx context.Context, hash []byte, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	if s.listChannelMessagesByHash != nil {
		return s.listChannelMessagesByHash(ctx, hash, since, limit, iatas, scope, cursor)
	}
	return api.Page[api.ChannelMessage]{}, nil
}

func (s stubReader) ListMessagesAfterID(ctx context.Context, afterID int64, iatas []string, scope string, limit int32) ([]api.ChannelMessage, error) {
	if s.listMessagesAfterID != nil {
		return s.listMessagesAfterID(ctx, afterID, iatas, scope, limit)
	}
	return nil, nil
}

func (s stubReader) ListObservers(ctx context.Context, iatas []string, observerType, broker, status, name, scope string, cursor int64, limit int32) (api.Page[api.ObserverSummary], error) {
	if s.listObservers != nil {
		return s.listObservers(ctx, iatas, observerType, broker, status, name, scope, cursor, limit)
	}
	return api.Page[api.ObserverSummary]{}, nil
}

func (s stubReader) GetObserver(ctx context.Context, observerID uuid.UUID) (*api.Observer, error) {
	if s.getObserver != nil {
		return s.getObserver(ctx, observerID)
	}
	return nil, nil
}

func (s stubReader) GetObserverTelemetry(ctx context.Context, observerID uuid.UUID, since, until time.Time, afterID int64) (*api.ObserverTelemetry, error) {
	if s.getObserverTelemetry != nil {
		return s.getObserverTelemetry(ctx, observerID, since, until, afterID)
	}
	return nil, nil
}

func (s stubReader) GetObserverTelemetryBucketed(ctx context.Context, observerID uuid.UUID, since, until time.Time, bucketHours int32) ([]api.ObserverTelemetryPoint, error) {
	if s.getObserverTelemetryBucketed != nil {
		return s.getObserverTelemetryBucketed(ctx, observerID, since, until, bucketHours)
	}
	return nil, nil
}

func (s stubReader) GetObserverScopes(ctx context.Context, observerID uuid.UUID) ([]string, error) {
	if s.getObserverScopes != nil {
		return s.getObserverScopes(ctx, observerID)
	}
	return nil, nil
}

func (s stubReader) ListObserverAdverts(ctx context.Context, observerID uuid.UUID, cursor int64, limit int32) (api.Page[api.AdvertObservation], error) {
	if s.listObserverAdverts != nil {
		return s.listObserverAdverts(ctx, observerID, cursor, limit)
	}
	return api.Page[api.AdvertObservation]{}, nil
}

func (s stubReader) ListNodes(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, pubkeyPrefix, name, scope string, cursor int64, limit int32, includeNeighbors bool) (api.Page[api.NodeSummary], error) {
	if s.listNodes != nil {
		return s.listNodes(ctx, nodeType, iatas, supportsMultibytePaths, supportsMultibyteTraces, pubkey, pubkeyPrefix, name, scope, cursor, limit, includeNeighbors)
	}
	return api.Page[api.NodeSummary]{}, nil
}

func (s stubReader) GetNode(ctx context.Context, nodeID uuid.UUID) (*api.Node, error) {
	if s.getNode != nil {
		return s.getNode(ctx, nodeID)
	}
	return nil, nil
}

func (s stubReader) GetNodeNeighbors(ctx context.Context, nodeID uuid.UUID) ([]api.NodeNeighbor, error) {
	if s.getNodeNeighbors != nil {
		return s.getNodeNeighbors(ctx, nodeID)
	}
	return nil, nil
}

func (s stubReader) ListNodeObservations(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (api.Page[api.PacketObservationSummary], error) {
	if s.listNodeObservations != nil {
		return s.listNodeObservations(ctx, nodeID, cursor, limit)
	}
	return api.Page[api.PacketObservationSummary]{}, nil
}

func (s stubReader) ListPackets(ctx context.Context, payloadTypes, routeTypes []int16, iatas []string, scopes []string, since, until time.Time, cursor int64, limit int32) (api.Page[api.PacketSummary], error) {
	if s.listPackets != nil {
		return s.listPackets(ctx, payloadTypes, routeTypes, iatas, scopes, since, until, cursor, limit)
	}
	return api.Page[api.PacketSummary]{}, nil
}

func (s stubReader) ListPacketsAfterID(ctx context.Context, afterObservationID int64, payloadType, routeType int16, iatas []string, scope string, limit int32) ([]api.PacketSummary, error) {
	if s.listPacketsAfterID != nil {
		return s.listPacketsAfterID(ctx, afterObservationID, payloadType, routeType, iatas, scope, limit)
	}
	return nil, nil
}

func (s stubReader) GetPacket(ctx context.Context, packetHash []byte) (*api.Packet, error) {
	if s.getPacket != nil {
		return s.getPacket(ctx, packetHash)
	}
	return nil, nil
}

func (s stubReader) GetRadioPresets(ctx context.Context, preset string, iatas []string) ([]api.RadioPreset, error) {
	if s.getRadioPresets != nil {
		return s.getRadioPresets(ctx, preset, iatas)
	}
	return nil, nil
}

func (s stubReader) GetStatsOverview(ctx context.Context, iatas []string) (*api.StatsOverview, error) {
	if s.getStatsOverview != nil {
		return s.getStatsOverview(ctx, iatas)
	}
	return nil, nil
}

func (s stubReader) GetStatsObservations(ctx context.Context, iatas []string, since time.Time) ([]api.ObservationPoint, error) {
	if s.getStatsObservations != nil {
		return s.getStatsObservations(ctx, iatas, since)
	}
	return nil, nil
}

func (s stubReader) GetStatsPayloadBreakdown(ctx context.Context, iatas []string, since time.Time) ([]api.PayloadBreakdownItem, error) {
	if s.getStatsPayloadBreakdown != nil {
		return s.getStatsPayloadBreakdown(ctx, iatas, since)
	}
	return nil, nil
}

func (s stubReader) GetStatsTopNodes(ctx context.Context, iatas []string, limit int32) ([]api.TopNode, error) {
	if s.getStatsTopNodes != nil {
		return s.getStatsTopNodes(ctx, iatas, limit)
	}
	return nil, nil
}

func (s stubReader) GetStatsTopObservers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopObserver, error) {
	if s.getStatsTopObservers != nil {
		return s.getStatsTopObservers(ctx, iatas, since, limit)
	}
	return nil, nil
}

func (s stubReader) GetStatsTopAdvertisers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopAdvertiser, error) {
	if s.getStatsTopAdvertisers != nil {
		return s.getStatsTopAdvertisers(ctx, iatas, since, limit)
	}
	return nil, nil
}

func (s stubReader) GetStatsTopTalkers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopTalker, error) {
	if s.getStatsTopTalkers != nil {
		return s.getStatsTopTalkers(ctx, iatas, since, limit)
	}
	return nil, nil
}

func (s stubReader) GetScopeStats(ctx context.Context) ([]api.ScopeStats, error) {
	if s.getScopeStats != nil {
		return s.getScopeStats(ctx)
	}
	return nil, nil
}

func (s stubReader) GetStatsNodeTypes(ctx context.Context, iatas []string) ([]api.NodeTypeCount, error) {
	if s.getStatsNodeTypes != nil {
		return s.getStatsNodeTypes(ctx, iatas)
	}
	return nil, nil
}

func (s stubReader) GetScopeNames(ctx context.Context) ([]string, error) {
	if s.getScopeNames != nil {
		return s.getScopeNames(ctx)
	}
	return nil, nil
}

func (s stubReader) GetScopesByIATAs(ctx context.Context, iatas []string) ([]api.ScopeSummary, error) {
	if s.getScopesByIATAs != nil {
		return s.getScopesByIATAs(ctx, iatas)
	}
	return nil, nil
}

func (s stubReader) GetScopeByName(ctx context.Context, name string) (*api.ScopeDetail, error) {
	if s.getScopeByName != nil {
		return s.getScopeByName(ctx, name)
	}
	return nil, nil
}

func (s stubReader) ListTraceTags(ctx context.Context, iatas []string, scope, traceType string, since, until time.Time, cursor time.Time, limit int32) ([]api.TraceTagSummary, error) {
	if s.listTraceTags != nil {
		return s.listTraceTags(ctx, iatas, scope, traceType, since, until, cursor, limit)
	}
	return nil, nil
}

func (s stubReader) GetTraceByTag(ctx context.Context, tag string) (*api.TraceDetail, error) {
	if s.getTraceByTag != nil {
		return s.getTraceByTag(ctx, tag)
	}
	return nil, nil
}

func (s stubReader) ListKnownRoutes(ctx context.Context, iata string, hopCount int32, cursor time.Time, limit int32) ([]api.KnownRoute, error) {
	if s.listKnownRoutes != nil {
		return s.listKnownRoutes(ctx, iata, hopCount, cursor, limit)
	}
	return nil, nil
}

func (s stubReader) SearchKnownRoutes(ctx context.Context, iata, fromHash, toHash string) ([]api.KnownRoute, error) {
	if s.searchKnownRoutes != nil {
		return s.searchKnownRoutes(ctx, iata, fromHash, toHash)
	}
	return nil, nil
}

func (s stubReader) GetKnownRoutesByNode(ctx context.Context, iata string, nodeID uuid.UUID) ([]api.KnownRoute, error) {
	if s.getKnownRoutesByNode != nil {
		return s.getKnownRoutesByNode(ctx, iata, nodeID)
	}
	return nil, nil
}

func (s stubReader) GetCrossIATANeighbors(ctx context.Context, nodeID uuid.UUID, iata string) ([]api.NodeNeighbor, error) {
	if s.getCrossIATANeighbors != nil {
		return s.getCrossIATANeighbors(ctx, nodeID, iata)
	}
	return nil, nil
}

func (s stubReader) SearchCrossIATARoutes(ctx context.Context, fromHash, fromIATA, toHash, toIATA string) ([]api.CrossIATARoute, error) {
	if s.searchCrossIATARoutes != nil {
		return s.searchCrossIATARoutes(ctx, fromHash, fromIATA, toHash, toIATA)
	}
	return nil, nil
}

func (s stubReader) GetNodesByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error) {
	if s.getNodesByIDs != nil {
		return s.getNodesByIDs(ctx, ids)
	}
	return nil, nil
}
