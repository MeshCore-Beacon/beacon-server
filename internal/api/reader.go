// Package api defines the response types and read interface for the Tower REST API.
package api

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// RadioPreset represents a unique radio configuration and where it is heard.
type RadioPreset struct {
	Preset     string `json:"preset"`
	IATA       string `json:"iata"`
	SourceType string `json:"sourceType"` // "observer" or "node"
	Count      int64  `json:"count"`
}

// StatsOverview is the top-level network summary for the overview endpoint.
type StatsOverview struct {
	TotalPackets      int64 `json:"totalPackets"`
	TotalObservations int64 `json:"totalObservations"`
	ActiveObservers   int64 `json:"activeObservers"`
	ActiveIATAs       int64 `json:"activeIatas"`
	WindowHours       int   `json:"windowHours"` // always 24 for now
}

// ObservationPoint is a single time-bucketed observation count.
type ObservationPoint struct {
	Hour             int64  `json:"hour"` // epoch ms, start of bucket
	IATA             string `json:"iata"`
	ObservationCount int64  `json:"observationCount"`
	UniquePackets    int64  `json:"uniquePackets"`
	ActiveObservers  int64  `json:"activeObservers"`
}

// PayloadBreakdownItem is a single payload type with its observation count.
type PayloadBreakdownItem struct {
	PayloadType     int16  `json:"payloadType"`
	PayloadTypeName string `json:"payloadTypeName"`
	Count           int64  `json:"count"`
}

// TopNode is a node ranked by observation count.
type TopNode struct {
	NodeID           uuid.UUID `json:"nodeId"`
	NodeName         *string   `json:"nodeName,omitempty"`
	NodeType         int16     `json:"nodeType"`
	NodeTypeName     string    `json:"nodeTypeName"`
	IATA             string    `json:"iata"`
	ObservationCount int64     `json:"observationCount"`
	LastHeard        int64     `json:"lastHeard"` // epoch ms
}

// TopObserver is an observer ranked by observation count.
type TopObserver struct {
	ObserverID       uuid.UUID `json:"observerId"`
	DisplayName      *string   `json:"displayName,omitempty"`
	ObserverType     *string   `json:"observerType,omitempty"`
	IATA             string    `json:"iata"`
	ObservationCount int64     `json:"observationCount"`
}

// PacketLatestObserver is the most recent observer summary rolled into a packet list item.
type PacketLatestObserver struct {
	ID          uuid.UUID `json:"id"`
	DisplayName *string   `json:"displayName,omitempty"`
	IATA        string    `json:"iata"`
}

// PacketSummary is the minimal packet representation used in list responses.
// Includes the latest observation rolled in for display purposes.
type PacketSummary struct {
	PacketHash       string                `json:"packetHash"` // hex-encoded
	PayloadType      int16                 `json:"payloadType"`
	PayloadTypeName  string                `json:"payloadTypeName"`
	RouteType        int16                 `json:"routeType"`
	RouteTypeName    string                `json:"routeTypeName"`
	Scope            *string               `json:"scope,omitempty"`
	FirstHeardAt     int64                 `json:"firstHeardAt"` // epoch ms
	LastHeardAt      int64                 `json:"lastHeardAt"`  // epoch ms
	ObservationCount int32                 `json:"observationCount"`
	LatestObserver   *PacketLatestObserver `json:"latestObserver,omitempty"`
	Summary          *string               `json:"summary,omitempty"` // human-readable payload summary
}

type PacketPathLength struct {
	Raw      string `json:"raw"`
	HashSize int16  `json:"hashSize"`
	HopCount int16  `json:"hopCount"`
}

// PacketObservationDetail is a full observation including radio settings and resolved path.
type PacketObservationDetail struct {
	ID                int64            `json:"id"`
	ObserverID        uuid.UUID        `json:"observerId"`
	ObserverName      *string          `json:"observerName,omitempty"`
	IATA              string           `json:"iata"`
	HeardAt           int64            `json:"heardAt"` // epoch ms
	PathLength        PacketPathLength `json:"pathLength"`
	PathBytes         *string          `json:"pathBytes,omitempty"` // hex-encoded
	RSSI              *int16           `json:"rssi,omitempty"`
	SNR               *float32         `json:"snr,omitempty"`
	PropagationTimeMs *int32           `json:"propagationTimeMs"`
	Radio             *PacketRadio     `json:"radio,omitempty"`
	SourceBroker      string           `json:"sourceBroker"`
	ResolvedPath      []ResolvedHop    `json:"resolvedPath"`
}

// PacketRadio holds the radio settings from the observation.
type PacketRadio struct {
	FreqMHz      *float32 `json:"freqMhz,omitempty"`
	SpreadFactor *int16   `json:"spreadFactor,omitempty"`
	BandwidthKHz *float32 `json:"bandwidthKhz,omitempty"`
	CodingRate   *int16   `json:"codingRate,omitempty"`
}

// ResolvedHop is a single hop in a packet's resolved path.
type ResolvedHop struct {
	Confidence string         `json:"confidence"` // "high", "low", "unknown"
	Nodes      []ResolvedNode `json:"nodes"`
}

// ResolvedNode is a node reference within a resolved path hop.
type ResolvedNode struct {
	ID        uuid.UUID `json:"id"`
	Name      *string   `json:"name,omitempty"`
	PublicKey string    `json:"publicKey"` // hex-encoded prefix
	Latitude  *float64  `json:"latitude,omitempty"`
	Longitude *float64  `json:"longitude,omitempty"`
}

type ResolvedPathEntry struct {
	NodeID    uuid.UUID
	Name      *string
	Latitude  *float64
	Longitude *float64
	PublicKey []byte
}

type PacketHeader struct {
	Raw             string `json:"raw"`
	RouteType       int16  `json:"routeType"`
	RouteTypeName   string `json:"routeTypeName"`
	PayloadType     int16  `json:"payloadType"`
	PayloadTypeName string `json:"payloadTypeName"`
	PayloadVersion  int16  `json:"payloadVersion"`
}

type PacketTransportCodes struct {
	RegionCode    int32 `json:"regionCode"`
	SubRegionCode int32 `json:"subRegionCode"`
}

// Packet is the full packet representation including all observations and resolved paths.
type Packet struct {
	PacketHash       string                    `json:"packetHash"`
	Header           PacketHeader              `json:"header"`
	TransportCodes   *PacketTransportCodes     `json:"transportCodes,omitempty"`
	OriginPubkey     *string                   `json:"originPubkey,omitempty"`
	ParsedPayload    json.RawMessage           `json:"parsedPayload,omitempty"`
	RawPayload       string                    `json:"rawPayload"`
	Decrypted        bool                      `json:"decrypted"`
	ChannelHash      *string                   `json:"channelHash,omitempty"`
	Scope            *string                   `json:"scope,omitempty"`
	FirstHeardAt     int64                     `json:"firstHeardAt"`
	LastHeardAt      int64                     `json:"lastHeardAt"`
	FirstToLastMs    int64                     `json:"firstToLastMs"`
	ObservationCount int32                     `json:"observationCount"`
	Observations     []PacketObservationDetail `json:"observations"`
}

// AdvertObservation extends PacketObservationSummary with node identity fields
// specific to advert packets (payload_type=4).
type AdvertObservation struct {
	PacketObservationSummary
	NodeName      *string `json:"nodeName,omitempty"`
	NodePublicKey *string `json:"nodePublicKey,omitempty"`
}

// PacketObservationSummary is a lightweight packet+observation pair used in
// list contexts such as observer adverts and node observations.
type PacketObservationSummary struct {
	ID              int64    `json:"id"`         // observation ID, use as cursor for pagination
	PacketHash      string   `json:"packetHash"` // hex-encoded
	PayloadType     int16    `json:"payloadType"`
	PayloadTypeName string   `json:"payloadTypeName"`
	IATA            string   `json:"iata"`
	HeardAt         int64    `json:"heardAt"` // epoch ms
	RSSI            *int16   `json:"rssi,omitempty"`
	SNR             *float32 `json:"snr,omitempty"`
	HopCount        *int16   `json:"hopCount,omitempty"`
}

// ObserverTelemetryPoint is a single telemetry snapshot for an observer.
type ObserverTelemetryPoint struct {
	T             int64    `json:"t"` // epoch ms
	BatteryMV     *int32   `json:"batteryMv,omitempty"`
	AirtimeTxPct  *float32 `json:"airtimeTxPct,omitempty"`
	AirtimeRxPct  *float32 `json:"airtimeRxPct,omitempty"`
	NoiseFloorDB  *float32 `json:"noiseFloorDb,omitempty"`
	UptimeSeconds *int64   `json:"uptimeSeconds,omitempty"`
	QueueLength   *int32   `json:"queueLength,omitempty"`
	ReceiveErrors *int32   `json:"receiveErrors,omitempty"`
}

// ObserverTelemetry is the full telemetry response for an observer.
// Range and interval reflect the query parameters used.
type ObserverTelemetry struct {
	Range    string                   `json:"range"`
	Interval string                   `json:"interval"`
	Points   []ObserverTelemetryPoint `json:"points"`
}

// NodeIATA represents a single IATA code and the last time the node was heard there.
type NodeIATA struct {
	IATA      string `json:"iata"`
	LastHeard int64  `json:"lastHeard"` // epoch ms
}

// NodeSummary is the minimal node representation used in list responses.
type NodeSummary struct {
	ID           uuid.UUID  `json:"id"`
	PublicKey    string     `json:"publicKey"` // hex-encoded public key
	NodeType     int16      `json:"nodeType"`  // 1=companion, 2=repeater, 3=room server
	NodeTypeName string     `json:"nodeTypeName"`
	Name         *string    `json:"name,omitempty"`
	IsObserver   bool       `json:"isObserver"`
	ObvserverID  *uuid.UUID `json:"observerId,omitempty"`
	Latitude     *float64   `json:"lat,omitempty"`
	Longitude    *float64   `json:"lng,omitempty"`
	Radio        *string    `json:"radio,omitempty"`
	IATAs        []NodeIATA `json:"iatas"`
	DefaultScope *string    `json:"defaultScope,omitempty"`
}

// Node is the full node representation including firmware capability flags,
// location source, and timing metadata.
type Node struct {
	NodeSummary
	LocationSource          *string `json:"locationSource,omitempty"`     // e.g. "advert", "manual"
	LastAdvertAt            *int64  `json:"lastAdvertAt,omitempty"`       // epoch ms, nil if no advert received
	SupportsMultibytePaths  bool    `json:"supportsMultibytePaths"`       // firmware >= 1.14.0
	SupportsMultibyteTraces bool    `json:"supportsMultibyteTraces"`      // firmware >= 1.11.0
	MinFirmwareVersion      *string `json:"minFirmwareVersion,omitempty"` // derived from capability flags
	FirstSeen               int64   `json:"firstSeen"`                    // epoch ms
	LastSeen                int64   `json:"lastSeen"`                     // epoch ms
	Metadata                any     `json:"metadata,omitempty"`           // raw JSONB metadata
}

// ObserverSummary is the minimal observer representation used in list responses.
type ObserverSummary struct {
	ID           uuid.UUID `json:"id"`
	DisplayName  *string   `json:"displayName,omitempty"`  // friendly name from /status messages
	ObserverType *string   `json:"observerType,omitempty"` // e.g. "meshcoretomqtt", "meshcoreha"
	IATA         string    `json:"iata"`                   // most recently heard IATA
	Status       string    `json:"status"`                 // "online" or "offline" derived from last_status_at
	Radio        *string   `json:"radio,omitempty"`        // friendly radio param string: freqMhz,BwKhz,SF
	Scopes       []string  `json:"scopes,omitempty"`       // list of observer forwarded scopes matched to config
}

// ObserverBroker represents a single MQTT broker an observer has been seen on,
// including timestamps for diagnosing partial outages — e.g. distinguishing
// "observer is down" from "one broker stopped delivering for this observer".
type ObserverBroker struct {
	Name         string `json:"name"`         // broker name e.g. "mqtt1"
	LastSeenAt   int64  `json:"lastSeenAt"`   // epoch ms, last time observer was seen on this broker
	LastPacketAt int64  `json:"lastPacketAt"` // epoch ms, last packet received via this broker; 0 if none
}

// Observer is the full observer representation including radio config,
// telemetry, broker memberships and raw status metadata.
type Observer struct {
	ObserverSummary
	PublicKey        string           `json:"publicKey"` // hex-encoded public key
	SoftwareVersion  *string          `json:"softwareVersion,omitempty"`
	HardwareModel    *string          `json:"hardwareModel,omitempty"`
	FirmwareVersion  *string          `json:"firmwareVersion,omitempty"`
	FirmwareBuild    *string          `json:"firmwareBuild,omitempty"`
	RadioFreqMHz     *float32         `json:"radioFreqMhz,omitempty"` // MHz e.g. 910.525
	RadioSF          *int16           `json:"radioSf,omitempty"`      // LoRa spreading factor
	RadioBWKHz       *float32         `json:"radioBwKhz,omitempty"`   // bandwidth in kHz
	RadioCR          *int16           `json:"radioCr,omitempty"`      // coding rate denominator
	BatteryLevel     *float32         `json:"batteryLevel,omitempty"` // volts, nil if mains powered
	UptimeSeconds    *int64           `json:"uptimeSeconds,omitempty"`
	StatusMetadata   any              `json:"statusMetadata,omitempty"` // raw /status JSON payload
	LastStatusAt     *int64           `json:"lastStatusAt,omitempty"`   // epoch ms
	FirstSeen        int64            `json:"firstSeen"`                // epoch ms
	LastSeen         int64            `json:"lastSeen"`                 // epoch ms
	ObservationCount int64            `json:"observationCount"`
	Brokers          []ObserverBroker `json:"brokers"` // broker names this observer has been seen on
}

// ChannelMessage represents a single decrypted channel message.
// Only messages for channels with a known key are stored and returned.
type ChannelMessage struct {
	ID               int64  `json:"id"`
	PacketHash       string `json:"packetHash"`       // hex-encoded packet hash for correlation with packet events
	ChannelHash      string `json:"channelHash"`      // hex-encoded single-byte channel hash
	SenderName       string `json:"senderName"`       // display name from the decrypted payload
	Content          string `json:"content"`          // decrypted message text
	SentAt           int64  `json:"sentAt"`           // epoch ms
	ObservationCount int64  `json:"observationCount"` // the number of observations for this message packet hash
}

// ChannelSummary is the minimal channel representation used in list responses.
type ChannelSummary struct {
	ID          int     `json:"id"`
	Name        *string `json:"name,omitempty"` // display name, nil if not set
	ChannelHash string  `json:"channelHash"`    // hex-encoded single-byte hash
	LastSeen    int64   `json:"lastSeen"`       // epoch ms
	IsHashtag   bool    `json:"isHashtag"`      // true if derived from a hashtag PSK
	KeyKnown    bool    `json:"keyKnown"`       // true if Tower has a decryption key
}

// Channel is the full channel representation including decryption metadata.
// KeyFingerprint is only populated for hashtag channels since their keys are
// publicly derivable from the tag name.
type Channel struct {
	ChannelSummary
	Hashtag        *string `json:"hashtag,omitempty"`        // tag name without # prefix
	KeyFingerprint *string `json:"keyFingerprint,omitempty"` // first 8 bytes of SHA256(key), hex-encoded
	MessageCount   int64   `json:"messageCount"`
}

// RegionSummary is the minimal region representation used in list responses.
type RegionSummary struct {
	ID   int    `json:"id"`
	Slug string `json:"slug"` // URL-safe identifier e.g. "western-canada"
	Name string `json:"name"`
}

// Region is the full region representation including map display hints and
// the list of IATA codes that are members of this region.
type Region struct {
	RegionSummary
	Description *string  `json:"description,omitempty"`
	CenterLat   *float64 `json:"centerLat,omitempty"` // map center latitude
	CenterLng   *float64 `json:"centerLng,omitempty"` // map center longitude
	ZoomLevel   *int     `json:"zoomLevel,omitempty"` // suggested map zoom level
	IATAs       []string `json:"iatas"`               // member IATA codes
}

// IATA represents a known airport/location code used to group observers and packets.
// DisplayName, Lat and Lng are optional — they are set via config file override
// or remain nil if the IATA was auto-created from packet traffic.
type IATA struct {
	IATA        string   `json:"iata"`
	DisplayName *string  `json:"displayName"`
	Lat         *float64 `json:"lat"`
	Lng         *float64 `json:"lon"`
}

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
	// ListNodeObservations returns a paginated list of packet observations originating from a node.
	// Pass cursor=0 to start from the beginning.
	ListNodeObservations(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (Page[PacketObservationSummary], error)
	// ListPackets returns a paginated list of packets with the latest observation rolled in.
	// Pass 0 for payloadType/routeType to skip those filters.
	// Pass nil for iatas, zero times for since/until to skip those filters.
	// cursor is last_heard_at epoch ms; pass 0 to start from the beginning.
	ListPackets(ctx context.Context, payloadType, routeType int16, iatas []string, scope string, since, until time.Time, cursor int64, limit int32) (Page[PacketSummary], error)
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
}
