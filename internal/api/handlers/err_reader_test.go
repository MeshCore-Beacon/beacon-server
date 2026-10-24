// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

// errReader is an api.Reader whose every method fails with err.
type errReader struct{ err error }

var _ api.Reader = errReader{}

func (e errReader) ListObserverDirectory(context.Context, api.ObserverDirectoryQuery) (v *api.ObserverDirectory, err error) {
	return v, e.err
}

func (e errReader) GetRouteEvidence(ctx context.Context, iata, pathKey string, query api.RouteEvidenceQuery) (v *api.RouteEvidence, err error) {
	return v, e.err
}

func (e errReader) ListIATAs(ctx context.Context) (v []api.IATA, err error) { return v, e.err }

func (e errReader) GetIATA(ctx context.Context, iata string) (v *api.IATA, err error) {
	return v, e.err
}

func (e errReader) GetIATABorder(ctx context.Context, iata string) (v json.RawMessage, err error) {
	return v, e.err
}

func (e errReader) ListRegions(ctx context.Context) (v []api.RegionSummary, err error) {
	return v, e.err
}

func (e errReader) GetRegion(ctx context.Context, regionID int32) (v *api.Region, err error) {
	return v, e.err
}

func (e errReader) GetRegionBySlug(ctx context.Context, slug string) (v *api.Region, err error) {
	return v, e.err
}

func (e errReader) ListChannels(ctx context.Context, limit int32, hash []byte, iatas []string, keyKnown *bool, cursor int64, pageCursor *api.ChannelCursor) (v api.ChannelPage, err error) {
	return v, e.err
}

func (e errReader) GetChannel(ctx context.Context, channelID int32) (v *api.Channel, err error) {
	return v, e.err
}

func (e errReader) ListChannelMessages(ctx context.Context, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (v api.Page[api.ChannelMessage], err error) {
	return v, e.err
}

func (e errReader) ListChannelMessagesByHash(ctx context.Context, hash []byte, since time.Time, limit int32, iatas []string, scope string, cursor int64) (v api.Page[api.ChannelMessage], err error) {
	return v, e.err
}

func (e errReader) ListMessagesAfterID(ctx context.Context, afterID int64, iatas []string, scope string, limit int32) (v []api.ChannelMessage, err error) {
	return v, e.err
}

func (e errReader) ListObservers(ctx context.Context, iatas []string, observerType, broker, status, name, scope string, cursor int64, pageCursor *api.ObserverCursor, limit int32) (v api.ObserverPage, err error) {
	return v, e.err
}

func (e errReader) GetObserver(ctx context.Context, observerID uuid.UUID) (v *api.Observer, err error) {
	return v, e.err
}

func (e errReader) GetObserverTelemetry(ctx context.Context, observerID uuid.UUID, since, until time.Time, afterID int64) (v *api.ObserverTelemetry, err error) {
	return v, e.err
}

func (e errReader) GetObserverTelemetryBucketed(ctx context.Context, observerID uuid.UUID, since, until time.Time, bucketHours int32) (v []api.ObserverTelemetryPoint, err error) {
	return v, e.err
}

func (e errReader) GetObserverActivity(ctx context.Context, observerID uuid.UUID, window, interval time.Duration, until time.Time) (v *api.ObserverActivity, err error) {
	return v, e.err
}

func (e errReader) GetObserverScopes(ctx context.Context, observerID uuid.UUID) (v []string, err error) {
	return v, e.err
}

func (e errReader) ListObserverAdverts(ctx context.Context, observerID uuid.UUID, cursor int64, limit int32) (v api.Page[api.AdvertObservation], err error) {
	return v, e.err
}

func (e errReader) ListNodes(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, pubkeyPrefix, name, scope string, cursor int64, pageCursor *api.NodeCursor, limit int32, includeNeighbors bool) (v api.NodePage, err error) {
	return v, e.err
}

func (e errReader) GetNode(ctx context.Context, nodeID uuid.UUID) (v *api.Node, err error) {
	return v, e.err
}

func (e errReader) GetNodesByIDs(ctx context.Context, ids []uuid.UUID) (v map[uuid.UUID]*api.ResolvedNode, err error) {
	return v, e.err
}

func (e errReader) ListNodeObservations(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (v api.Page[api.PacketObservationSummary], err error) {
	return v, e.err
}

func (e errReader) ListPackets(ctx context.Context, payloadTypes, routeTypes []int16, iatas []string, scopes []string, since, until time.Time, cursor int64, pageCursor *api.PacketCursor, limit int32) (v api.PacketPage, err error) {
	return v, e.err
}

func (e errReader) SearchPacketsByHashPrefix(ctx context.Context, lo, hi []byte, payloadTypes, routeTypes []int16, iatas []string, scopes []string, since, until time.Time, cursor int64, limit int32) (v api.Page[api.PacketSummary], err error) {
	return v, e.err
}

func (e errReader) ListPacketsAfterID(ctx context.Context, afterObservationID int64, payloadType, routeType int16, iatas []string, scope string, limit int32) (v []api.PacketSummary, err error) {
	return v, e.err
}

func (e errReader) GetPacket(ctx context.Context, packetHash []byte) (v *api.Packet, err error) {
	return v, e.err
}

func (e errReader) GetRadioPresets(ctx context.Context, preset string, iatas []string) (v []api.RadioPreset, err error) {
	return v, e.err
}

func (e errReader) GetObserverComparison(ctx context.Context, observerA, observerB uuid.UUID, since, until time.Time, iatas []string) (v *api.ObserverComparison, err error) {
	return v, e.err
}

func (e errReader) GetStatsOverview(ctx context.Context, iatas []string) (v *api.StatsOverview, err error) {
	return v, e.err
}

func (e errReader) GetStatsObservations(ctx context.Context, iatas []string, since time.Time) (v []api.ObservationPoint, err error) {
	return v, e.err
}

func (e errReader) GetStatsSeries(ctx context.Context, since, until time.Time, iatas []string) (v *api.StatsSeries, err error) {
	return v, e.err
}

func (e errReader) GetSignalStats(ctx context.Context, since, until time.Time, iatas []string) (v *api.SignalStats, err error) {
	return v, e.err
}

func (e errReader) GetPathStats(ctx context.Context, since, until time.Time, iatas []string) (v *api.PathStats, err error) {
	return v, e.err
}

func (e errReader) GetStatsPayloadBreakdown(ctx context.Context, iatas []string, since time.Time) (v []api.PayloadBreakdownItem, err error) {
	return v, e.err
}

func (e errReader) GetStatsTopNodes(ctx context.Context, iatas []string, since time.Time, limit int32) (v []api.TopNode, err error) {
	return v, e.err
}

func (e errReader) GetStatsTopObservers(ctx context.Context, iatas []string, since time.Time, limit int32) (v []api.TopObserver, err error) {
	return v, e.err
}

func (e errReader) GetStatsTopAdvertisers(ctx context.Context, iatas []string, since time.Time, limit int32, sort api.AdvertiserSort) (v []api.TopAdvertiser, err error) {
	return v, e.err
}

func (e errReader) GetStatsClockDrift(ctx context.Context, iatas []string, limit int32) (v []api.ClockDriftEntry, err error) {
	return v, e.err
}

func (e errReader) GetStatsTopTalkers(ctx context.Context, iatas []string, since time.Time, limit int32) (v []api.TopTalker, err error) {
	return v, e.err
}

func (e errReader) GetScopeStats(ctx context.Context, iatas []string, since time.Time) (v []api.ScopeStats, err error) {
	return v, e.err
}

func (e errReader) GetStatsNodeTypes(ctx context.Context, iatas []string) (v []api.NodeTypeCount, err error) {
	return v, e.err
}

func (e errReader) GetScopeNames(ctx context.Context) (v []string, err error) { return v, e.err }

func (e errReader) GetScopeByName(ctx context.Context, name string) (v *api.ScopeDetail, err error) {
	return v, e.err
}

func (e errReader) ListTraceTags(ctx context.Context, iatas []string, scope, traceType string, since, until time.Time, cursor time.Time, cursorTag string, limit int32) (v []api.TraceTagSummary, err error) {
	return v, e.err
}

func (e errReader) GetTraceByTag(ctx context.Context, tag string) (v *api.TraceDetail, err error) {
	return v, e.err
}

func (e errReader) ListKnownRoutes(ctx context.Context, iata string, hopCount int32, cursor time.Time, cursorID int64, limit int32) (v []api.KnownRoute, err error) {
	return v, e.err
}

func (e errReader) SearchKnownRoutes(ctx context.Context, iatas []string, fromHash, toHash string) (v []api.KnownRoute, err error) {
	return v, e.err
}

func (e errReader) GetNodeNeighbors(ctx context.Context, nodeID uuid.UUID) (v []api.NodeNeighbor, err error) {
	return v, e.err
}

func (e errReader) SearchCrossIATARoutes(ctx context.Context, q api.CrossRouteSearch) (v []api.CrossIATARoute, err error) {
	return v, e.err
}
