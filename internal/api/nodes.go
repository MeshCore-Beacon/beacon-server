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
