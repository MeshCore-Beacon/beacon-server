package api

import (
	"encoding/json"
	"strings"

	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

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
	Scope            *string               `json:"scope,omitempty"`            // matched transport scope name e.g. "#bc"
	FirstHeardAt     int64                 `json:"firstHeardAt"`               // epoch ms
	LastHeardAt      int64                 `json:"lastHeardAt"`                // epoch ms
	ObservationCount int32                 `json:"observationCount"`
	LatestObserver   *PacketLatestObserver `json:"latestObserver,omitempty"`
	Summary          *string               `json:"summary,omitempty"` // human-readable payload summary
}

// PacketPathLength is the decoded path_length byte from a packet observation.
// The raw byte encodes both hash size and hop count in a bit-packed format (§2.5).
type PacketPathLength struct {
	Raw      string `json:"raw"`      // hex-encoded single byte
	HashSize int16  `json:"hashSize"` // per-hop hash size in bytes (1, 2, or 3)
	HopCount int16  `json:"hopCount"` // number of path hashes present
}

// PacketObservationDetail is a full observation including radio settings and resolved path.
type PacketObservationDetail struct {
	ID                int64            `json:"id"`
	ObserverID        uuid.UUID        `json:"observerId"`
	ObserverName      *string          `json:"observerName,omitempty"`
	IATA              string           `json:"iata"`
	HeardAt           int64            `json:"heardAt"` // epoch ms
	PathLength        PacketPathLength `json:"pathLength"`
	PathBytes         *string          `json:"pathBytes,omitempty"` // hex-encoded accumulated path hashes
	RSSI              *int16           `json:"rssi,omitempty"`
	SNR               *float32         `json:"snr,omitempty"`
	PropagationTimeMs *int32           `json:"propagationTimeMs"` // ms since first observation; 0 for first
	Radio             *PacketRadio     `json:"radio,omitempty"`
	SourceBroker      string           `json:"sourceBroker"`
	ResolvedPath      []ResolvedHop    `json:"resolvedPath"` // per-observation resolved path hashes
}

// PacketRadio holds the radio settings copied from the observer at observation time.
type PacketRadio struct {
	FreqMHz      *float32 `json:"freqMhz,omitempty"`
	SpreadFactor *int16   `json:"spreadFactor,omitempty"`
	BandwidthKHz *float32 `json:"bandwidthKhz,omitempty"`
	CodingRate   *int16   `json:"codingRate,omitempty"`
}

// ResolvedHop is a single hop in a packet's resolved path.
// Confidence is "high" (exactly one match), "ambiguous" (multiple matches), or "none" (no match).
type ResolvedHop struct {
	Confidence string         `json:"confidence"` // "high", "ambiguous", or "none"
	Nodes      []ResolvedNode `json:"nodes"`      // empty for "none", one for "high", multiple for "ambiguous"
}

// ResolvedNode is a node reference within a resolved path hop.
type ResolvedNode struct {
	ID        uuid.UUID `json:"id"`
	Name      *string   `json:"name,omitempty"`
	PublicKey string    `json:"publicKey"` // hex-encoded prefix used for resolution
	Latitude  *float64  `json:"latitude,omitempty"`
	Longitude *float64  `json:"longitude,omitempty"`
}

// ResolvedPathEntry is an internal type used by the store layer to carry node
// details returned from ResolvePathHashes before mapping to ResolvedNode.
type ResolvedPathEntry struct {
	NodeID    uuid.UUID
	Name      *string
	Latitude  *float64
	Longitude *float64
	PublicKey []byte
}

// PacketHeader holds the decoded header byte and its bit-packed fields.
// The raw header byte encodes payload version, payload type, and route type (§2.3).
type PacketHeader struct {
	Raw             string `json:"raw"`             // hex-encoded single byte
	RouteType       int16  `json:"routeType"`       // bits 0-1
	RouteTypeName   string `json:"routeTypeName"`   // FLOOD, DIRECT, TRANSPORT_FLOOD, TRANSPORT_DIRECT
	PayloadType     int16  `json:"payloadType"`     // bits 2-5
	PayloadTypeName string `json:"payloadTypeName"` // advert, request, group_text, etc.
	PayloadVersion  int16  `json:"payloadVersion"`  // bits 6-7
}

// PacketTransportCodes holds the decoded transport codes present in TRANSPORT_FLOOD
// and TRANSPORT_DIRECT packets. RegionCode is transport_code_1; SubRegionCode is
// transport_code_2 (reserved in v1, always 0 on the wire).
type PacketTransportCodes struct {
	RegionCode    int32 `json:"regionCode"`
	SubRegionCode int32 `json:"subRegionCode"`
}

// Packet is the full packet representation including all observations and resolved paths.
type Packet struct {
	PacketHash       string                    `json:"packetHash"`
	Header           PacketHeader              `json:"header"`
	TransportCodes   *PacketTransportCodes     `json:"transportCodes,omitempty"`
	OriginPubkey     *string                   `json:"originPubkey,omitempty"` // hex-encoded; nil when not extractable from payload
	ParsedPayload    json.RawMessage           `json:"parsedPayload,omitempty"`
	RawPayload       string                    `json:"rawPayload"`              // hex-encoded payload bytes (excludes header and path)
	Decrypted        bool                      `json:"decrypted"`               // true if group text was successfully decrypted
	ChannelHash      *string                   `json:"channelHash,omitempty"`   // hex-encoded single byte; non-nil for group_text/group_data
	Scope            *string                   `json:"scope,omitempty"`         // matched transport scope name e.g. "#bc"
	FirstHeardAt     int64                     `json:"firstHeardAt"`            // epoch ms
	LastHeardAt      int64                     `json:"lastHeardAt"`             // epoch ms
	FirstToLastMs    int64                     `json:"firstToLastMs"`           // ms between first and last observation
	ObservationCount int32                     `json:"observationCount"`
	ResolvedRoute    []ResolvedHop             `json:"resolvedRoute,omitempty"` // trace packets only: resolved intended route
	Observations     []PacketObservationDetail `json:"observations"`
}

// AdvertObservation extends PacketObservationSummary with node identity fields
// specific to advert packets (payload_type=4).
type AdvertObservation struct {
	PacketObservationSummary
	NodeName      *string `json:"nodeName,omitempty"`
	NodePublicKey *string `json:"nodePublicKey,omitempty"` // hex-encoded
}

// PacketObservationSummary is a lightweight packet+observation pair used in
// list contexts such as observer adverts and node observations.
type PacketObservationSummary struct {
	ID              int64    `json:"id"`          // observation ID, use as cursor for pagination
	PacketHash      string   `json:"packetHash"`  // hex-encoded
	PayloadType     int16    `json:"payloadType"`
	PayloadTypeName string   `json:"payloadTypeName"`
	IATA            string   `json:"iata"`
	HeardAt         int64    `json:"heardAt"` // epoch ms
	RSSI            *int16   `json:"rssi,omitempty"`
	SNR             *float32 `json:"snr,omitempty"`
	HopCount        *int16   `json:"hopCount,omitempty"`
}

// PayloadTypeName returns a human-readable name for a payload type integer.
func PayloadTypeName(t int16) string {
	switch t {
	case 0x00:
		return "request"
	case 0x01:
		return "response"
	case 0x02:
		return "text_message"
	case 0x03:
		return "acknowledgement"
	case 0x04:
		return "advert"
	case 0x05:
		return "group_text"
	case 0x06:
		return "group_data"
	case 0x07:
		return "anonymous_request"
	case 0x08:
		return "path"
	case 0x09:
		return "trace"
	case 0x0A:
		return "multipart"
	case 0x0B:
		return "control"
	case 0x0C:
		fallthrough
	case 0x0D:
		fallthrough
	case 0x0E:
		return "reserved"
	case 0x0F:
		return "raw_custom"
	default:
		return "unknown"
	}
}

// PayloadTypeFromString returns the integer payload type for a given name.
// Returns -1 (no filter) if the string is empty or unrecognized.
func PayloadTypeFromString(s string) int16 {
	switch strings.ToLower(s) {
	case "request", "req":
		return int16(meshcore.PayloadTypeReq)
	case "response":
		return int16(meshcore.PayloadTypeResponse)
	case "txt_msg", "txtmsg", "text", "direct":
		return int16(meshcore.PayloadTypeTxtMsg)
	case "acknowledgement", "ack":
		return int16(meshcore.PayloadTypeAck)
	case "advertisement", "advert":
		return int16(meshcore.PayloadTypeAdvert)
	case "grp_txt", "grptxt", "group_text", "group":
		return int16(meshcore.PayloadTypeGrpTxt)
	case "grp_data", "grpdata", "group_data", "data":
		return int16(meshcore.PayloadTypeGrpData)
	case "anonymous_request", "anon_req", "anonreq":
		return int16(meshcore.PayloadTypeAnonReq)
	case "path":
		return int16(meshcore.PayloadTypePath)
	case "trace":
		return int16(meshcore.PayloadTypeTrace)
	case "multipart", "multi-part":
		return int16(meshcore.PayloadTypeMultiPart)
	case "control":
		return int16(meshcore.PayloadTypeControl)
	case "raw_custom", "raw", "custom":
		return int16(meshcore.PayloadTypeRawCustom)
	default:
		return -1
	}
}

// RouteTypeName returns a human-readable name for a route type integer.
func RouteTypeName(t int16) string {
	switch byte(t) {
	case meshcore.RouteTypeFlood:
		return "FLOOD"
	case meshcore.RouteTypeDirect:
		return "DIRECT"
	case meshcore.RouteTypeTransportFlood:
		return "TRANSPORT_FLOOD"
	case meshcore.RouteTypeTransportDirect:
		return "TRANSPORT_DIRECT"
	default:
		return "unknown"
	}
}
