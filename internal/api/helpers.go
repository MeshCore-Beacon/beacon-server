package api

import (
	"strings"

	"github.com/meshcore-go/meshcore-go"
)

// PayloadTypeName returns a human-readable name for a payload type integer.
func PayloadTypeName(t int16) string {
	switch t {
	case 0:
		return "raw"
	case 1:
		return "txt_msg"
	case 2:
		return "sensor_data"
	case 4:
		return "advert"
	case 5:
		return "grp_txt"
	case 8:
		return "sign"
	case 9:
		return "trace"
	default:
		return "unknown"
	}
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
