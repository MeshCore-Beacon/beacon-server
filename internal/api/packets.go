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
	ResolvedRoute    []ResolvedHop             `json:"resolvedRoute,omitempty"`
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
