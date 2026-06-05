package api

import (
	"strings"

	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

// NodeIATA represents a single IATA code and the last time the node was heard there.
type NodeIATA struct {
	IATA      string `json:"iata"`
	LastHeard int64  `json:"lastHeard"` // epoch ms
}

// NodeSummary is the minimal node representation used in list responses.
type NodeSummary struct {
	ID           uuid.UUID  `json:"id"`
	PublicKey    string     `json:"publicKey"` // hex-encoded Ed25519 public key
	NodeType     int16      `json:"nodeType"`  // 1=companion, 2=repeater, 3=room_server, 4=sensor
	NodeTypeName string     `json:"nodeTypeName"`
	Name         *string    `json:"name,omitempty"`
	IsObserver   bool       `json:"isObserver"`             // true if this node is also a known observer
	ObserverID   *uuid.UUID `json:"observerId,omitempty"`   // UUID of the associated observer row, if any
	Latitude     *float64   `json:"lat,omitempty"`          // decimal degrees, from advert AppData
	Longitude    *float64   `json:"lng,omitempty"`          // decimal degrees, from advert AppData
	Radio        *string    `json:"radio,omitempty"`        // shorthand: "freqMhz,bwKhz,sf" e.g. "910.5,62.5,7"
	IATAs        []NodeIATA `json:"iatas"`                  // IATAs where this node has been heard, with last heard timestamps
	DefaultScope *string    `json:"defaultScope,omitempty"` // most recently matched transport scope name e.g. "#bc"
}

// Node is the full node representation including firmware capability flags,
// location source, and timing metadata.
type Node struct {
	NodeSummary
	LocationSource          *string `json:"locationSource,omitempty"`     // "advert" or "manual"
	LastAdvertAt            *int64  `json:"lastAdvertAt,omitempty"`       // epoch ms, nil if no advert received
	SupportsMultibytePaths  bool    `json:"supportsMultibytePaths"`       // firmware >= 1.14.0; detected via path hash size
	SupportsMultibyteTraces bool    `json:"supportsMultibyteTraces"`      // firmware >= 1.11.0; detected via trace hash size
	MinFirmwareVersion      *string `json:"minFirmwareVersion,omitempty"` // derived from capability flags
	FirstSeen               int64   `json:"firstSeen"`                    // epoch ms
	LastSeen                int64   `json:"lastSeen"`                     // epoch ms
	Metadata                any     `json:"metadata,omitempty"`           // raw JSONB metadata
}

// NodeTypeName returns a human-readable name for a node type integer.
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
