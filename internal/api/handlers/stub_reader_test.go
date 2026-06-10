// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

// stubReader satisfies api.Reader with zero-value returns.
// Use it for handler tests that exercise validation paths where
// the reader is never actually called.
type stubReader struct{}

func (stubReader) ListIATAs(ctx context.Context) ([]api.IATA, error) {
	return nil, nil
}

func (stubReader) GetIATA(ctx context.Context, iata string) (*api.IATA, error) {
	return nil, nil
}

func (stubReader) ListRegions(ctx context.Context) ([]api.RegionSummary, error) {
	return nil, nil
}

func (stubReader) GetRegion(ctx context.Context, regionID int32) (*api.Region, error) {
	return nil, nil
}

func (stubReader) GetRegionBySlug(ctx context.Context, slug string) (*api.Region, error) {
	return nil, nil
}

func (stubReader) ListChannels(ctx context.Context, limit int32, hash []byte, iata string, cursor int64) (api.Page[api.ChannelSummary], error) {
	return api.Page[api.ChannelSummary]{}, nil
}

func (stubReader) GetChannel(ctx context.Context, channelID int32) (*api.Channel, error) {
	return nil, nil
}

func (stubReader) ListChannelMessages(ctx context.Context, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	return api.Page[api.ChannelMessage]{}, nil
}

func (stubReader) ListChannelMessagesByHash(ctx context.Context, hash []byte, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	return api.Page[api.ChannelMessage]{}, nil
}

func (stubReader) ListMessagesAfterID(ctx context.Context, afterID int64, iatas []string, scope string, limit int32) ([]api.ChannelMessage, error) {
	return nil, nil
}

func (stubReader) ListObservers(ctx context.Context, iatas []string, observerType, broker, status, name, scope string, cursor int64, limit int32) (api.Page[api.ObserverSummary], error) {
	return api.Page[api.ObserverSummary]{}, nil
}

func (stubReader) GetObserver(ctx context.Context, observerID uuid.UUID) (*api.Observer, error) {
	return nil, nil
}

func (stubReader) GetObserverTelemetry(ctx context.Context, observerID uuid.UUID, since, until time.Time, afterID int64) (*api.ObserverTelemetry, error) {
	return nil, nil
}

func (stubReader) GetObserverTelemetryBucketed(ctx context.Context, observerID uuid.UUID, since, until time.Time, bucketHours int32) ([]api.ObserverTelemetryPoint, error) {
	return nil, nil
}

func (stubReader) GetObserverScopes(ctx context.Context, observerID uuid.UUID) ([]string, error) {
	return nil, nil
}

func (stubReader) ListObserverAdverts(ctx context.Context, observerID uuid.UUID, cursor int64, limit int32) (api.Page[api.AdvertObservation], error) {
	return api.Page[api.AdvertObservation]{}, nil
}

func (stubReader) ListNodes(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, name, scope string, cursor int64, limit int32) (api.Page[api.NodeSummary], error) {
	return api.Page[api.NodeSummary]{}, nil
}

func (stubReader) GetNode(ctx context.Context, nodeID uuid.UUID) (*api.Node, error) {
	return nil, nil
}

func (stubReader) ListNodeObservations(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (api.Page[api.PacketObservationSummary], error) {
	return api.Page[api.PacketObservationSummary]{}, nil
}

func (stubReader) ListPackets(ctx context.Context, payloadType, routeType int16, iatas []string, scope string, since, until time.Time, cursor int64, limit int32) (api.Page[api.PacketSummary], error) {
	return api.Page[api.PacketSummary]{}, nil
}

func (stubReader) ListPacketsAfterID(ctx context.Context, afterObservationID int64, payloadType, routeType int16, iatas []string, scope string, limit int32) ([]api.PacketSummary, error) {
	return nil, nil
}

func (stubReader) GetPacket(ctx context.Context, packetHash []byte) (*api.Packet, error) {
	return nil, nil
}

func (stubReader) GetRadioPresets(ctx context.Context, preset string, iatas []string) ([]api.RadioPreset, error) {
	return nil, nil
}

func (stubReader) GetStatsOverview(ctx context.Context, iatas []string) (*api.StatsOverview, error) {
	return nil, nil
}

func (stubReader) GetStatsObservations(ctx context.Context, iatas []string, since time.Time) ([]api.ObservationPoint, error) {
	return nil, nil
}

func (stubReader) GetStatsPayloadBreakdown(ctx context.Context, iatas []string, since time.Time) ([]api.PayloadBreakdownItem, error) {
	return nil, nil
}

func (stubReader) GetStatsTopNodes(ctx context.Context, iatas []string, limit int32) ([]api.TopNode, error) {
	return nil, nil
}

func (stubReader) GetStatsTopObservers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopObserver, error) {
	return nil, nil
}

func (stubReader) GetScopeStats(ctx context.Context) ([]api.ScopeStats, error) {
	return nil, nil
}

func (stubReader) GetStatsNodeTypes(ctx context.Context, iatas []string) ([]api.NodeTypeCount, error) {
	return nil, nil
}

func (stubReader) GetScopeNames(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (stubReader) GetScopesByIATAs(ctx context.Context, iatas []string) ([]api.ScopeSummary, error) {
	return nil, nil
}

func (stubReader) GetScopeByName(ctx context.Context, name string) (*api.ScopeDetail, error) {
	return nil, nil
}

func (stubReader) ListTraceTags(ctx context.Context, iatas []string, scope string, since, until time.Time, cursor time.Time, limit int32) ([]api.TraceTagSummary, error) {
	return nil, nil
}

func (stubReader) GetTraceByTag(ctx context.Context, tag string) (*api.TraceDetail, error) {
	return nil, nil
}

func (stubReader) ListKnownRoutes(ctx context.Context, iata string, hopCount int32, cursor time.Time, limit int32) ([]api.KnownRoute, error) {
	return nil, nil
}

func (stubReader) SearchKnownRoutes(ctx context.Context, iata, fromHash, toHash string) ([]api.KnownRoute, error) {
	return nil, nil
}

func (stubReader) GetNodeNeighbors(ctx context.Context, nodeID uuid.UUID) ([]api.NodeNeighbor, error) {
	return nil, nil
}

func (stubReader) GetKnownRoutesByNode(ctx context.Context, iata string, nodeID uuid.UUID) ([]api.KnownRoute, error) {
	return nil, nil
}

func (stubReader) GetCrossIATANeighbors(ctx context.Context, nodeID uuid.UUID, iata string) ([]api.NodeNeighbor, error) {
	return nil, nil
}

func (stubReader) SearchCrossIATARoutes(ctx context.Context, fromHash, fromIATA, toHash, toIATA string) ([]api.CrossIATARoute, error) {
	return nil, nil
}

func (stubReader) GetNodesByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error) {
	return nil, nil
}
