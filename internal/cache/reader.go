// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

const (
	keyIATAs                   = "beacon:iatas"
	keyIATAPrefix              = "beacon:iata:"
	keyIATABorderPrefix        = "beacon:iata:border:"
	keyRegions                 = "beacon:regions"
	keyRegionPrefix            = "beacon:region:"
	keyRegionSlugPrefix        = "beacon:region:slug:"
	keyScopeNames              = "beacon:scope:names"
	keyScopeStats              = "beacon:scope:stats"
	keyScopeByNamePrefix       = "beacon:scope:name:"
	keyStatsOverviewPrefix     = "beacon:stats:overview:"
	keyStatsObservationsPrefix = "beacon:stats:observations:"
	keySignalStatsPrefix       = "beacon:stats:signal:"
	keyStatsSeriesPrefix       = "beacon:stats:series:"
	keyPathStatsPrefix         = "beacon:stats:paths:"
	keyStatsBreakdownPrefix    = "beacon:stats:breakdown:"
	keyStatsTopNodesPrefix     = "beacon:stats:top-nodes:"
	keyStatsTopObsPrefix       = "beacon:stats:top-observers:"
	keyStatsTopAdvPrefix       = "beacon:stats:top-advertisers:"
	keyStatsClockDriftPrefix   = "beacon:stats:clock-drift:"
	keyStatsTopTalkersPrefix   = "beacon:stats:top-talkers:"
	keyStatsNodeTypes          = "beacon:stats:node-types:"
	keyRadioPresetsPrefix      = "beacon:radio-presets:"
	keyNodePrefix              = "beacon:node:"
	keyNodeNeighborsPrefix     = "beacon:node:neighbors:"
	keyNodesByIDsPrefix        = "beacon:nodes:ids:"
	keyObserverPrefix          = "beacon:observer:"
	keyObserverScopesPrefix    = "beacon:observer:scopes:"
	keyObserverActivityPrefix  = "beacon:observer:activity:"
)

// observerActivityTTL is deliberately short: activity is a live chart, not reference data.
const observerActivityTTL = 60 * time.Second

// CachedReader wraps an api.Reader with a Redis caching layer.
// It implements api.Reader and is a drop-in replacement for db.Store
// at the wiring point in main.go.
type CachedReader struct {
	inner api.Reader
	c     *Client
	ttl   CacheTTLs
	rev   revisionMemo
	now   func() time.Time
}

// CacheTTLs holds the resolved per-category TTLs for the cache layer.
// All fields should be non-zero — use ResolveTTLs to build this from
// config with fallback to the global TTL and then the default.
type CacheTTLs struct {
	Stats     time.Duration
	Reference time.Duration
	Nodes     time.Duration
	Observers time.Duration
}

// NewCachedReader returns an api.Reader that transparently caches responses
// using the provided Redis client and TTL configuration. inner is the
// underlying db.Store that is called on cache misses.
func NewCachedReader(inner api.Reader, c *Client, ttl CacheTTLs) api.Reader {
	return &CachedReader{
		inner: inner,
		c:     c,
		ttl:   ttl,
		now:   time.Now,
	}
}

// InvalidateScopeNames makes newly committed catalogue names visible to every scope read.
func (cr *CachedReader) InvalidateScopeNames(ctx context.Context) {
	cr.c.del(ctx, keyScopeNames)
	cr.c.delPrefix(ctx, keyScopeByNamePrefix)
	cr.c.delPrefix(ctx, keyScopeStats)
}

// InvalidateIATABorder makes an imported or removed MeshMapper boundary visible.
func (cr *CachedReader) InvalidateIATABorder(ctx context.Context, iata string) {
	cr.c.del(ctx, keyIATABorderPrefix+iata)
}

// InvalidateIATAs makes imported MeshMapper names and locations visible to every IATA read.
func (cr *CachedReader) InvalidateIATAs(ctx context.Context) {
	cr.c.del(ctx, keyIATAs)
	cr.c.delPrefix(ctx, keyIATAPrefix)
}

// InvalidateRegions makes imported MeshMapper regions visible to every region read.
func (cr *CachedReader) InvalidateRegions(ctx context.Context) {
	cr.c.del(ctx, keyRegions)
	cr.c.delPrefix(ctx, keyRegionPrefix)
}

// InvalidateNode removes the cached entries for a node by UUID.
// Should be called from the ingest path after a node upsert.
func (cr *CachedReader) InvalidateNode(ctx context.Context, nodeID uuid.UUID) {
	id := nodeID.String()
	cr.c.del(ctx, keyNodePrefix+id, keyNodeNeighborsPrefix+id)
}

// InvalidateObserver removes the cached entries for an observer by UUID.
// Should be called from the ingest path after an observer upsert.
func (cr *CachedReader) InvalidateObserver(ctx context.Context, observerID uuid.UUID) {
	id := observerID.String()
	cr.c.del(ctx, keyObserverPrefix+id, keyObserverScopesPrefix+id)
}

// ListIATAs implements [api.Reader].
func (cr *CachedReader) ListIATAs(ctx context.Context) ([]api.IATA, error) {
	return getOrSet(ctx, cr.c, keyIATAs, cr.ttl.Reference, func(ctx context.Context) ([]api.IATA, error) {
		return cr.inner.ListIATAs(ctx)
	})
}

// GetIATA implements [api.Reader].
func (cr *CachedReader) GetIATA(ctx context.Context, iata string) (*api.IATA, error) {
	return getOrSet(ctx, cr.c, keyIATAPrefix+iata, cr.ttl.Reference, func(ctx context.Context) (*api.IATA, error) {
		return cr.inner.GetIATA(ctx, iata)
	})
}

// GetIATABorder implements [api.Reader].
func (cr *CachedReader) GetIATABorder(ctx context.Context, iata string) (json.RawMessage, error) {
	return getOrSet(ctx, cr.c, keyIATABorderPrefix+iata, cr.ttl.Reference, func(ctx context.Context) (json.RawMessage, error) {
		return cr.inner.GetIATABorder(ctx, iata)
	})
}

// ListRegions implements [api.Reader].
func (cr *CachedReader) ListRegions(ctx context.Context) ([]api.RegionSummary, error) {
	return getOrSet(ctx, cr.c, keyRegions, cr.ttl.Reference, func(ctx context.Context) ([]api.RegionSummary, error) {
		return cr.inner.ListRegions(ctx)
	})
}

// GetRegion implements [api.Reader].
func (cr *CachedReader) GetRegion(ctx context.Context, regionID int32) (*api.Region, error) {
	return getOrSet(ctx, cr.c, fmt.Sprintf("%s%d", keyRegionPrefix, regionID), cr.ttl.Reference, func(ctx context.Context) (*api.Region, error) {
		return cr.inner.GetRegion(ctx, regionID)
	})
}

// GetRegionBySlug implements [api.Reader].
func (cr *CachedReader) GetRegionBySlug(ctx context.Context, slug string) (*api.Region, error) {
	return getOrSet(ctx, cr.c, keyRegionSlugPrefix+slug, cr.ttl.Reference, func(ctx context.Context) (*api.Region, error) {
		return cr.inner.GetRegionBySlug(ctx, slug)
	})
}

// GetScopeNames implements [api.Reader].
func (cr *CachedReader) GetScopeNames(ctx context.Context) ([]string, error) {
	return getOrSet(ctx, cr.c, keyScopeNames, cr.ttl.Reference, func(ctx context.Context) ([]string, error) {
		return cr.inner.GetScopeNames(ctx)
	})
}

// GetObserverComparison passes arbitrary observer/time combinations through;
// these explicit queries should not fill the shared statistics cache.
func (cr *CachedReader) GetObserverComparison(ctx context.Context, a, b uuid.UUID, since, until time.Time, iatas []string) (*api.ObserverComparison, error) {
	return cr.inner.GetObserverComparison(ctx, a, b, since, until, iatas)
}

// GetStatsNodeTypes implements [api.Reader].
func (cr *CachedReader) GetStatsNodeTypes(ctx context.Context, iatas []string) ([]api.NodeTypeCount, error) {
	segment := "all"
	if len(iatas) > 0 {
		sorted := append([]string(nil), iatas...)
		sort.Strings(sorted)
		segment = strings.Join(sorted, ",")
	}
	key := fmt.Sprintf("%s%s", keyStatsNodeTypes, segment)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(ctx context.Context) ([]api.NodeTypeCount, error) {
		return cr.inner.GetStatsNodeTypes(ctx, iatas)
	})
}

// GetStatsClockDrift implements [api.Reader].
func (cr *CachedReader) GetStatsClockDrift(ctx context.Context, iatas []string, limit int32) ([]api.ClockDriftEntry, error) {
	segment := "all"
	if len(iatas) > 0 {
		sorted := append([]string(nil), iatas...)
		sort.Strings(sorted)
		segment = strings.Join(sorted, ",")
	}
	key := fmt.Sprintf("%s%s:%d", keyStatsClockDriftPrefix, segment, limit)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(ctx context.Context) ([]api.ClockDriftEntry, error) {
		return cr.inner.GetStatsClockDrift(ctx, iatas, limit)
	})
}

// GetRadioPresets implements [api.Reader].
func (cr *CachedReader) GetRadioPresets(ctx context.Context, preset string, iatas []string) ([]api.RadioPreset, error) {
	segment := "all"
	if len(iatas) > 0 {
		sorted := append([]string(nil), iatas...)
		sort.Strings(sorted)
		segment = strings.Join(sorted, ",")
	}
	key := fmt.Sprintf("%s%s:%s", keyRadioPresetsPrefix, preset, segment)
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func(ctx context.Context) ([]api.RadioPreset, error) {
		return cr.inner.GetRadioPresets(ctx, preset, iatas)
	})
}

// GetNode implements [api.Reader].
func (cr *CachedReader) GetNode(ctx context.Context, nodeID uuid.UUID) (*api.Node, error) {
	return getOrSet(ctx, cr.c, keyNodePrefix+nodeID.String(), cr.ttl.Nodes, func(ctx context.Context) (*api.Node, error) {
		return cr.inner.GetNode(ctx, nodeID)
	})
}

// GetNodeNeighbors implements [api.Reader].
func (cr *CachedReader) GetNodeNeighbors(ctx context.Context, nodeID uuid.UUID) ([]api.NodeNeighbor, error) {
	return getOrSet(ctx, cr.c, keyNodeNeighborsPrefix+nodeID.String(), cr.ttl.Nodes, func(ctx context.Context) ([]api.NodeNeighbor, error) {
		return cr.inner.GetNodeNeighbors(ctx, nodeID)
	})
}

// GetNodesByIDs implements [api.Reader].
func (cr *CachedReader) GetNodesByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error) {
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = id.String()
	}
	sort.Strings(strs)
	key := keyNodesByIDsPrefix + strings.Join(strs, ",")
	return getOrSet(ctx, cr.c, key, cr.ttl.Nodes, func(ctx context.Context) (map[uuid.UUID]*api.ResolvedNode, error) {
		return cr.inner.GetNodesByIDs(ctx, ids)
	})
}

// GetObserver implements [api.Reader].
func (cr *CachedReader) GetObserver(ctx context.Context, observerID uuid.UUID) (*api.Observer, error) {
	return getOrSet(ctx, cr.c, keyObserverPrefix+observerID.String(), cr.ttl.Observers, func(ctx context.Context) (*api.Observer, error) {
		return cr.inner.GetObserver(ctx, observerID)
	})
}

// GetObserverScopes implements [api.Reader].
func (cr *CachedReader) GetObserverScopes(ctx context.Context, observerID uuid.UUID) ([]string, error) {
	return getOrSet(ctx, cr.c, keyObserverScopesPrefix+observerID.String(), cr.ttl.Observers, func(ctx context.Context) ([]string, error) {
		return cr.inner.GetObserverScopes(ctx, observerID)
	})
}

// GetObserverTelemetry implements [api.Reader].
func (cr *CachedReader) GetObserverTelemetry(ctx context.Context, observerID uuid.UUID, since, until time.Time, afterID int64) (*api.ObserverTelemetry, error) {
	return cr.inner.GetObserverTelemetry(ctx, observerID, since, until, afterID)
}

// GetObserverTelemetryBucketed implements [api.Reader].
func (cr *CachedReader) GetObserverTelemetryBucketed(ctx context.Context, observerID uuid.UUID, since, until time.Time, bucketHours int32) ([]api.ObserverTelemetryPoint, error) {
	return cr.inner.GetObserverTelemetryBucketed(ctx, observerID, since, until, bucketHours)
}

// GetObserverActivity implements [api.Reader].
func (cr *CachedReader) GetObserverActivity(ctx context.Context, observerID uuid.UUID, window, interval time.Duration, until time.Time) (*api.ObserverActivity, error) {
	if !until.IsZero() {
		until = until.UTC().Truncate(interval)
	}
	key := keyObserverActivityPrefix + observerID.String() + ":" + window.String() + ":" + interval.String() + ":" + until.UTC().Format(time.RFC3339Nano)
	return getOrSet(ctx, cr.c, key, observerActivityTTL, func(ctx context.Context) (*api.ObserverActivity, error) {
		return cr.inner.GetObserverActivity(ctx, observerID, window, interval, until)
	})
}

// GetPacket implements [api.Reader].
func (cr *CachedReader) GetPacket(ctx context.Context, packetHash []byte) (*api.Packet, error) {
	return cr.inner.GetPacket(ctx, packetHash)
}

// GetChannel implements [api.Reader].
func (cr *CachedReader) GetChannel(ctx context.Context, channelID int32) (*api.Channel, error) {
	return cr.inner.GetChannel(ctx, channelID)
}

// GetTraceByTag implements [api.Reader].
func (cr *CachedReader) GetTraceByTag(ctx context.Context, tag string) (*api.TraceDetail, error) {
	return cr.inner.GetTraceByTag(ctx, tag)
}

// GetKnownRoutesByNode implements [api.Reader].
func (cr *CachedReader) GetKnownRoutesByNode(ctx context.Context, iata string, nodeID uuid.UUID) ([]api.KnownRoute, error) {
	return cr.inner.GetKnownRoutesByNode(ctx, iata, nodeID)
}

// GetCrossIATANeighbors implements [api.Reader].
func (cr *CachedReader) GetCrossIATANeighbors(ctx context.Context, nodeID uuid.UUID, iata string) ([]api.NodeNeighbor, error) {
	return cr.inner.GetCrossIATANeighbors(ctx, nodeID, iata)
}

// ListChannels implements [api.Reader].
func (cr *CachedReader) ListChannels(ctx context.Context, limit int32, hash []byte, iatas []string, keyKnown *bool, cursor int64, pageCursor *api.ChannelCursor) (api.ChannelPage, error) {
	return cr.inner.ListChannels(ctx, limit, hash, iatas, keyKnown, cursor, pageCursor)
}

// ListChannelMessages implements [api.Reader].
func (cr *CachedReader) ListChannelMessages(ctx context.Context, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	return cr.inner.ListChannelMessages(ctx, channelID, since, limit, iatas, scope, cursor)
}

// ListChannelMessagesByHash implements [api.Reader].
func (cr *CachedReader) ListChannelMessagesByHash(ctx context.Context, hash []byte, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	return cr.inner.ListChannelMessagesByHash(ctx, hash, since, limit, iatas, scope, cursor)
}

// ListMessagesAfterID implements [api.Reader].
func (cr *CachedReader) ListMessagesAfterID(ctx context.Context, afterID int64, iatas []string, scope string, limit int32) ([]api.ChannelMessage, error) {
	return cr.inner.ListMessagesAfterID(ctx, afterID, iatas, scope, limit)
}

// ListNodes implements [api.Reader].
func (cr *CachedReader) ListNodes(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, pubkeyPrefix, name, scope string, cursor int64, limit int32, includeNeighbors bool) (api.Page[api.NodeSummary], error) {
	return cr.inner.ListNodes(ctx, nodeType, iatas, supportsMultibytePaths, supportsMultibyteTraces, pubkey, pubkeyPrefix, name, scope, cursor, limit, includeNeighbors)
}

// ListNodeObservations implements [api.Reader].
func (cr *CachedReader) ListNodeObservations(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (api.Page[api.PacketObservationSummary], error) {
	return cr.inner.ListNodeObservations(ctx, nodeID, cursor, limit)
}

// ListObservers implements [api.Reader].
func (cr *CachedReader) ListObservers(ctx context.Context, iatas []string, observerType, broker, status, name, scope string, cursor int64, limit int32) (api.Page[api.ObserverSummary], error) {
	return cr.inner.ListObservers(ctx, iatas, observerType, broker, status, name, scope, cursor, limit)
}

// ListObserverAdverts implements [api.Reader].
func (cr *CachedReader) ListObserverAdverts(ctx context.Context, observerID uuid.UUID, cursor int64, limit int32) (api.Page[api.AdvertObservation], error) {
	return cr.inner.ListObserverAdverts(ctx, observerID, cursor, limit)
}

// ListPackets implements [api.Reader].
func (cr *CachedReader) ListPackets(ctx context.Context, payloadTypes, routeTypes []int16, iatas []string, scopes []string, since, until time.Time, cursor int64, limit int32) (api.Page[api.PacketSummary], error) {
	return cr.inner.ListPackets(ctx, payloadTypes, routeTypes, iatas, scopes, since, until, cursor, limit)
}

// ListPacketsAfterID implements [api.Reader].
func (cr *CachedReader) ListPacketsAfterID(ctx context.Context, afterObservationID int64, payloadType, routeType int16, iatas []string, scope string, limit int32) ([]api.PacketSummary, error) {
	return cr.inner.ListPacketsAfterID(ctx, afterObservationID, payloadType, routeType, iatas, scope, limit)
}

// ListKnownRoutes implements [api.Reader].
func (cr *CachedReader) ListKnownRoutes(ctx context.Context, iata string, hopCount int32, cursor time.Time, cursorID int64, limit int32) ([]api.KnownRoute, error) {
	return cr.inner.ListKnownRoutes(ctx, iata, hopCount, cursor, cursorID, limit)
}

// SearchKnownRoutes implements [api.Reader].
func (cr *CachedReader) SearchKnownRoutes(ctx context.Context, iata, fromHash, toHash string) ([]api.KnownRoute, error) {
	return cr.inner.SearchKnownRoutes(ctx, iata, fromHash, toHash)
}

// Precise cursor/window combinations are intentionally passed through, like route lists.
func (cr *CachedReader) GetRouteEvidence(ctx context.Context, iata, key string, query api.RouteEvidenceQuery) (*api.RouteEvidence, error) {
	return cr.inner.GetRouteEvidence(ctx, iata, key, query)
}

// SearchCrossIATARoutes implements [api.Reader].
func (cr *CachedReader) SearchCrossIATARoutes(ctx context.Context, fromHash, fromIATA, toHash, toIATA string) ([]api.CrossIATARoute, error) {
	return cr.inner.SearchCrossIATARoutes(ctx, fromHash, fromIATA, toHash, toIATA)
}

// ListTraceTags implements [api.Reader].
func (cr *CachedReader) ListTraceTags(ctx context.Context, iatas []string, scope, traceType string, since, until time.Time, cursor time.Time, cursorTag string, limit int32) ([]api.TraceTagSummary, error) {
	return cr.inner.ListTraceTags(ctx, iatas, scope, traceType, since, until, cursor, cursorTag, limit)
}
