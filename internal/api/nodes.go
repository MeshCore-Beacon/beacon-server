// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"strings"

	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

// NodeNeighbor represents a neighboring node relationship observed in a given IATA.
type NodeNeighbor struct {
	ID               uuid.UUID `json:"id"`
	Name             *string   `json:"name,omitempty"`
	PublicKey        string    `json:"publicKey"`
	NodeType         int16     `json:"nodeType"`
	NodeTypeName     string    `json:"nodeTypeName"`
	Latitude         *float64  `json:"lat,omitempty"`
	Longitude        *float64  `json:"lng,omitempty"`
	IATA             string    `json:"iata"`
	ObservationCount int64     `json:"observationCount"`
	FirstSeen        int64     `json:"firstSeen"` // epoch ms
	LastSeen         int64     `json:"lastSeen"`  // epoch ms
	SNR              *float32  `json:"snr,omitempty"`
}

// NodeIATA represents a single IATA code and the last time the node was heard there.
type NodeIATA struct {
	IATA      string `json:"iata"`
	LastHeard int64  `json:"lastHeard"` // epoch ms
}

// NodePage adds the exact cursor while keeping the numeric one for older clients.
type NodePage struct {
	Page[NodeSummary]
	NextPageCursor *string `json:"nextPageCursor,omitempty"`
	// Sort and Order name the order the items are in ("lastSeen" "desc" unless a sort was asked
	// for), so a client can tell this server sorted; older servers leave them out.
	Sort  string `json:"sort,omitempty" enums:"lastSeen,name,type,neighbors"`
	Order string `json:"order,omitempty" enums:"asc,desc"`
}

// Node list orders besides the default, last seen newest first.
const (
	NodeSortName      = "name"
	NodeSortType      = "type"
	NodeSortNeighbors = "neighbors"
)

// NodeSort orders a node list by name, type or neighbour count.
type NodeSort struct {
	By         string
	Descending bool
}

// ValidNodeSort reports whether by is a NodeSort order.
func ValidNodeSort(by string) bool {
	return by == NodeSortName || by == NodeSortType || by == NodeSortNeighbors
}

// NodeSummary is the minimal node representation used in list responses.
type NodeSummary struct {
	ID                 uuid.UUID   `json:"id"`
	PublicKey          string      `json:"publicKey"` // hex-encoded Ed25519 public key
	NodeType           int16       `json:"nodeType"`  // 1=companion, 2=repeater, 3=room_server, 4=sensor
	NodeTypeName       string      `json:"nodeTypeName"`
	Name               *string     `json:"name,omitempty"`
	IsObserver         bool        `json:"isObserver"`             // true if this node is also a known observer
	ObserverID         *uuid.UUID  `json:"observerId,omitempty"`   // UUID of the associated observer row, if any
	Latitude           *float64    `json:"lat,omitempty"`          // decimal degrees, from advert AppData
	Longitude          *float64    `json:"lng,omitempty"`          // decimal degrees, from advert AppData
	Radio              *string     `json:"radio,omitempty"`        // shorthand: "freqMhz,bwKhz,sf" e.g. "910.525,62.5,7"
	IATAs              []NodeIATA  `json:"iatas"`                  // IATAs where this node has been heard, with last heard timestamps
	DefaultScope       *string     `json:"defaultScope,omitempty"` // most recently matched transport scope name e.g. "#bc"
	KnownNeighborCount int64       `json:"knownNeighborCount"`
	NeighborIDs        []uuid.UUID `json:"neighborIds,omitempty"` // only populated when the list request opts in; see ?neighbors=true
	// Stale is true when the node hasn't been seen (last_seen) within the configured
	// staleness window (default 24h; internal/config.ResolvedConfig.NodeStaleThreshold).
	// Applies to every node type, unlike ClockDriftSeconds/ClockOutOfSync on Node, which
	// are repeater/room-server only.
	Stale bool `json:"stale"`
	// PossiblyForeign compares a repeater's stored position with this server's
	// configured IATA border union. Omitted when disabled, unknown, or not repeater.
	PossiblyForeign *bool `json:"possiblyForeign,omitempty"`
}

// Node is the full node representation including firmware capability flags,
// location source, and timing metadata.
type Node struct {
	NodeSummary
	LocationSource          *string        `json:"locationSource,omitempty"`     // "advert" or "manual"
	LastAdvertAt            *int64         `json:"lastAdvertAt,omitempty"`       // epoch ms, nil if no advert received
	SupportsMultibytePaths  bool           `json:"supportsMultibytePaths"`       // firmware >= 1.14.0; detected via path hash size
	SupportsMultibyteTraces bool           `json:"supportsMultibyteTraces"`      // firmware >= 1.11.0; detected via trace hash size
	MinFirmwareVersion      *string        `json:"minFirmwareVersion,omitempty"` // derived from capability flags
	FirstSeen               int64          `json:"firstSeen"`                    // epoch ms
	LastSeen                int64          `json:"lastSeen"`                     // epoch ms
	Metadata                any            `json:"metadata,omitempty"`           // raw JSONB metadata
	Neighbors               []NodeNeighbor `json:"neighbors"`
	// Clock drift, repeaters/room servers only (nodeType 2/3); omitted entirely for other
	// node types or when no qualifying advert has been measured yet. Device minus server
	// time, in seconds, from the advert's self-reported timestamp: +ve = device ahead.
	// clockCheckedAt is the server receive time of the advert this was measured from (same
	// moment as lastAdvertAt). clockOutOfSync is |clockDriftSeconds| exceeding a configured
	// threshold (default 5m) -- see internal/config.ResolvedConfig.ClockDriftThreshold.
	ClockDriftSeconds *int   `json:"clockDriftSeconds,omitempty"`
	ClockOutOfSync    *bool  `json:"clockOutOfSync,omitempty"`
	ClockCheckedAt    *int64 `json:"clockCheckedAt,omitempty"`
	// MeshMapper is what MeshMapper's repeater list says about this key; omitted when
	// the import is off or MeshMapper doesn't list it.
	MeshMapper *MeshMapperRepeater `json:"meshmapper,omitempty"`
}

// MeshMapperRepeater is imported from MeshMapper's get_repeaters.php. Times are epoch ms.
// Text fields are admin-entered free text: escape before rendering.
type MeshMapperRepeater struct {
	IATA            string                  `json:"iata,omitempty"` // region of its latest advert, per MeshMapper
	FetchedAt       int64                   `json:"fetchedAt"`      // when Beacon last fetched this list
	Site            MeshMapperSite          `json:"site"`
	Carpeater       bool                    `json:"carpeater"` // mobile repeater: its position moves
	Admins          []string                `json:"admins"`
	Scopes          []MeshMapperScope       `json:"scopes"`
	ScopesCheckedAt *int64                  `json:"scopesCheckedAt,omitempty"` // set (with empty scopes) when it answered with none
	DefaultScope    *MeshMapperDefaultScope `json:"defaultScope,omitempty"`
}

// MeshMapperSite holds admin-entered site details; each field is omitted when unset.
type MeshMapperSite struct {
	Power       *string  `json:"power,omitempty"`
	Hardware    *string  `json:"hardware,omitempty"`
	Antenna     *string  `json:"antenna,omitempty"`
	HeightM     *float64 `json:"heightM,omitempty"`     // antenna above ground
	HeightASLM  *float64 `json:"heightAslM,omitempty"`  // antenna above mean sea level
	PowerSource *string  `json:"powerSource,omitempty"` // mains, solar, battery or poe
	Notes       *string  `json:"notes,omitempty"`
}

// MeshMapperScope is one scope the repeater was confirmed carrying, and how.
type MeshMapperScope struct {
	Scope           string  `json:"scope"`
	ObservedAt      *int64  `json:"observedAt,omitempty"` // seen forwarding scoped traffic
	ObservedPackets *int64  `json:"observedPackets,omitempty"`
	Proof           *string `json:"proof,omitempty"`      // multibyte or floor
	ReportedAt      *int64  `json:"reportedAt,omitempty"` // an observer asked it
	UploadedAt      *int64  `json:"uploadedAt,omitempty"` // a wardriver's app asked it
}

// MeshMapperDefaultScope is the scope the repeater puts on its own adverts.
type MeshMapperDefaultScope struct {
	Name      *string `json:"name,omitempty"`
	State     string  `json:"state"` // named, unknown (set but not nameable yet) or none
	UpdatedAt *int64  `json:"updatedAt,omitempty"`
	Source    *string `json:"source,omitempty"` // advert or reported
}

// NodeTypeName returns a human-readable name for a node type integer.
// NOTE: truncation is fine here until there are at least over 200 types of node
func NodeTypeName(t int16) string {
	switch byte(t) {
	case meshcore.AdvertTypeChat:
		return "companion"
	case meshcore.AdvertTypeRepeater:
		return "repeater"
	case meshcore.AdvertTypeRoom:
		return "room_server"
	case meshcore.AdvertTypeSensor:
		return "sensor"
	default:
		return "unknown"
	}
}

// NodeTypeFromString returns the integer node type for a given name.
// Returns 0 (no filter) if the string is empty or unrecognized.
func NodeTypeFromString(s string) int16 {
	switch strings.ToLower(s) {
	case "companion", "chat":
		return int16(meshcore.AdvertTypeChat)
	case "repeater":
		return int16(meshcore.AdvertTypeRepeater)
	case "room_server", "roomserver", "room-server", "room":
		return int16(meshcore.AdvertTypeRoom)
	case "sensor":
		return int16(meshcore.AdvertTypeSensor)
	default:
		return 0
	}
}
