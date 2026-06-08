// Package api defines the response types and read interface for the Beacon REST API.
package api

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Page is a generic paginated response envelope used by all list endpoints
// that support cursor-based pagination. NextCursor is the ID of the last item
// returned and should be passed as the cursor param in the next request.
// HasMore is true when additional results exist beyond the current page.
type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor *int64 `json:"nextCursor,omitempty"`
	HasMore    bool   `json:"hasMore"`
}

type Reader interface {
	// ListIATAs returns all known IATA codes with display name and coordinates.
	// IATAs are auto-created on first packet arrival from that location.
	ListIATAs(ctx context.Context) ([]IATA, error)

	// GetIATA returns a single IATA code by its 3-letter identifier.
	// Returns nil, error if the IATA code is not found.
	GetIATA(ctx context.Context, iata string) (*IATA, error)

	// ListRegions returns a summary list of all regions ordered by display_order then name.
	// Use GetRegion for full detail including associated IATAs.
	ListRegions(ctx context.Context) ([]RegionSummary, error)

	// GetRegion returns full detail for a single region including its associated IATA codes.
	// Returns nil, pgx.ErrNoRows if the region is not found.
	GetRegion(ctx context.Context, regionID int32) (*Region, error)

	// GetRegionBySlug returns full detail for a single region by its URL-safe slug.
	// Returns nil, pgx.ErrNoRows if the region is not found.
	GetRegionBySlug(ctx context.Context, slug string) (*Region, error)

	// ListChannels returns a paginated list of channels ordered by last seen.
	// Includes both hashtag-derived and explicit key channels.
	// Pass nil hash to skip hash filtering. Pass empty string iata to return all channels.
	// cursor is last_seen epoch ms of the last item; pass 0 to start from the beginning.
	ListChannels(ctx context.Context, limit int32, hash []byte, iata string, cursor int64) (Page[ChannelSummary], error)

	// GetChannel returns full detail for a single channel by its integer ID.
	// Returns nil, pgx.ErrNoRows if the channel is not found.
	GetChannel(ctx context.Context, channelID int32) (*Channel, error)

	// ListChannelMessages returns paginated messages for a channel identified by its integer ID.
	// Used by the /channels/{id}/messages endpoint.
	// Pass a zero time.Time for since to return all messages up to limit.
	// Pass empty string iata to return messages from all IATAs.
	// Pass cursor=0 to start from the beginning.
	ListChannelMessages(ctx context.Context, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (Page[ChannelMessage], error)

	// ListChannelMessagesByHash returns paginated messages for all channels matching the given hash.
	// Used by the /messages?hash= endpoint. May return messages from multiple channels
	// if the hash collides across different keys.
	// Pass a zero time.Time for since to return all messages up to limit.
	// Pass empty string iata to return messages from all IATAs.
	// Pass cursor=0 to start from the beginning.
	ListChannelMessagesByHash(ctx context.Context, hash []byte, since time.Time, limit int32, iatas []string, scope string, cursor int64) (Page[ChannelMessage], error)

	// ListMessagesAfterID returns channel messages after the given message ID,
	// ordered oldest first. Used for WS reconnect backfill.
	ListMessagesAfterID(ctx context.Context, afterID int64, iatas []string, scope string, limit int32) ([]ChannelMessage, error)

	// ListObservers returns a paginated list of observers with optional filters.
	// All filter params are optional — pass empty string or nil to skip a filter.
	// status is "online" or "offline" derived from last_status_at recency.
	// cursor is last_seen epoch ms of the last observer; pass 0 to start from the beginning.
	ListObservers(ctx context.Context, iatas []string, observerType, broker, status, name, scope string, cursor int64, limit int32) (Page[ObserverSummary], error)

	// GetObserver returns full detail for a single observer by UUID.
	// Returns nil, pgx.ErrNoRows if the observer is not found.
	GetObserver(ctx context.Context, observerID uuid.UUID) (*Observer, error)

	// GetObserverTelemetry returns telemetry points for an observer within the given time range.
	// since and until define the window; pass zero times to use defaults (last 24h).
	GetObserverTelemetry(ctx context.Context, observerID uuid.UUID, since, until time.Time, afterID int64) (*ObserverTelemetry, error)
	// TODO: add interval time.Duration param for server-side bucketing

	// GetObserverScopes returns the names of all transport scopes an observer has
	// been seen forwarding packets for, ordered alphabetically.
	GetObserverScopes(ctx context.Context, observerID uuid.UUID) ([]string, error)

	// ListObserverAdverts returns a paginated list of advert packets heard by an observer.
	// Pass cursor=0 to start from the beginning.
	ListObserverAdverts(ctx context.Context, observerID uuid.UUID, cursor int64, limit int32) (Page[AdvertObservation], error)

	// ListNodes returns a paginated list of nodes with optional filters.
	// Pass 0 for nodeType, nil iatas, nil for pubkey to skip those filters.
	// cursor is last_seen epoch ms; pass 0 to start from the beginning.
	ListNodes(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, name, scope string, cursor int64, limit int32) (Page[NodeSummary], error)

	// GetNode returns full detail for a single node by UUID.
	// Returns nil, pgx.ErrNoRows if the node is not found.
	GetNode(ctx context.Context, nodeID uuid.UUID) (*Node, error)

	// GetNodesByIDs returns a map of node ID to resolved node details for the given IDs.
	GetNodesByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*ResolvedNode, error)

	// ListNodeObservations returns a paginated list of packet observations originating from a node.
	// Pass cursor=0 to start from the beginning.
	ListNodeObservations(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (Page[PacketObservationSummary], error)

	// ListPackets returns a paginated list of packets with the latest observation rolled in.
	// Pass 0 for payloadType/routeType to skip those filters.
	// Pass nil for iatas, zero times for since/until to skip those filters.
	// cursor is last_heard_at epoch ms; pass 0 to start from the beginning.
	ListPackets(ctx context.Context, payloadType, routeType int16, iatas []string, scope string, since, until time.Time, cursor int64, limit int32) (Page[PacketSummary], error)

	// ListPacketsAfterID returns packets with observations after the given observation ID,
	// ordered oldest first. Used for WS reconnect backfill.
	ListPacketsAfterID(ctx context.Context, afterObservationID int64, payloadType, routeType int16, iatas []string, scope string, limit int32) ([]PacketSummary, error)

	// GetPacket returns full packet detail including all observations with radio settings.
	// Returns nil, pgx.ErrNoRows if not found.
	GetPacket(ctx context.Context, packetHash []byte) (*Packet, error)
	// GetRadioPresets returns radio preset usage grouped by preset and IATA.
	// Pass empty string for preset or iata to skip those filters.
	GetRadioPresets(ctx context.Context, preset, iata string) ([]RadioPreset, error)

	// GetStatsOverview returns top-line network figures for the last 24 hours.
	// Pass empty string iata to return stats across all IATAs.
	GetStatsOverview(ctx context.Context, iata string) (*StatsOverview, error)

	// GetStatsObservations returns hourly observation counts for charting.
	// Pass empty string iata to return stats across all IATAs.
	// since defines the start of the window; pass zero time for default (last 7 days).
	GetStatsObservations(ctx context.Context, iata string, since time.Time) ([]ObservationPoint, error)

	// GetStatsPayloadBreakdown returns observation counts grouped by payload type.
	// Pass empty string iata to return stats across all IATAs.
	// since defines the start of the window; pass zero time for default (last 24h).
	GetStatsPayloadBreakdown(ctx context.Context, iata string, since time.Time) ([]PayloadBreakdownItem, error)

	// GetStatsTopNodes returns the top N nodes by observation count.
	// Pass empty string iata to return stats across all IATAs.
	GetStatsTopNodes(ctx context.Context, iata string, limit int32) ([]TopNode, error)

	// GetStatsTopObservers returns the top N observers by observation count.
	// Pass empty string iata to return stats across all IATAs.
	// since defines the start of the window; pass zero time for default (last 24h).
	GetStatsTopObservers(ctx context.Context, iata string, since time.Time, limit int32) ([]TopObserver, error)

	// GetScopeStats returns aggregate packet, observer and node counts per transport scope.
	GetScopeStats(ctx context.Context) ([]ScopeStats, error)

	// GetScopeNames returns the names of all configured transport scopes, ordered alphabetically.
	// Use when no geographic filter is applied — returns names only for a lightweight response.
	GetScopeNames(ctx context.Context) ([]string, error)

	// GetScopesByIATAs returns scope summaries filtered by the given IATA codes,
	// including observer, node and IATA counts. Expands region/regionId to IATAs automatically.
	GetScopesByIATAs(ctx context.Context, iatas []string) ([]ScopeSummary, error)

	// GetScopeByName returns full detail for a single scope by its normalized name (e.g. "#bc"),
	// including packet count, observer count, node count, and the list of IATAs it is active in.
	// Returns nil if the scope is not found.
	GetScopeByName(ctx context.Context, name string) (*ScopeDetail, error)

	// ListTraceTags returns a paginated list of trace tags with aggregate metadata.
	ListTraceTags(ctx context.Context, iatas []string, scope string, since, until time.Time, cursor time.Time, limit int32) ([]TraceTagSummary, error)

	// GetTraceByTag returns all packets for a given trace tag with resolved routes.
	GetTraceByTag(ctx context.Context, tag string) (*TraceDetail, error)

	// ListKnownRoutes returns known routes filtered by IATA and optional hop count.
	ListKnownRoutes(ctx context.Context, iata string, hopCount int32, cursor time.Time, limit int32) ([]KnownRoute, error)

	// SearchKnownRoutes returns known routes containing a path from source to destination hash.
	SearchKnownRoutes(ctx context.Context, iata, fromHash, toHash string) ([]KnownRoute, error)

	// GetNodeNeighbors returns the neighbors of a node ordered by most recently seen.
	GetNodeNeighbors(ctx context.Context, nodeID uuid.UUID) ([]NodeNeighbor, error)

	// GetKnownRoutesByNode returns all known routes in a given IATA that contain
	// the specified node UUID anywhere in their hop sequence.
	GetKnownRoutesByNode(ctx context.Context, iata string, nodeID uuid.UUID) ([]KnownRoute, error)

	// GetCrossIATANeighbors returns neighbors of a node that were observed in a
	// different IATA — indicating a potential cross-IATA radio link.
	GetCrossIATANeighbors(ctx context.Context, nodeID uuid.UUID, iata string) ([]NodeNeighbor, error)

	// SearchCrossIATARoutes finds routes that cross IATA boundaries between
	// a source node/IATA and a destination node/IATA.
	SearchCrossIATARoutes(ctx context.Context, fromHash, fromIATA, toHash, toIATA string) ([]CrossIATARoute, error)
}
