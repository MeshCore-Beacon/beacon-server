package api

import (
	"strings"

	"github.com/meshcore-go/meshcore-go"
)

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
