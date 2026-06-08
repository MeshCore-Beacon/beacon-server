// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api_test

import (
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/meshcore-go/meshcore-go"
)

func TestPayloadTypeName(t *testing.T) {
	tests := []struct {
		input int16
		want  string
	}{
		{0x00, "request"},
		{0x01, "response"},
		{0x02, "text_message"},
		{0x03, "acknowledgement"},
		{0x04, "advert"},
		{0x05, "group_text"},
		{0x06, "group_data"},
		{0x07, "anonymous_request"},
		{0x08, "path"},
		{0x09, "trace"},
		{0x0A, "multipart"},
		{0x0B, "control"},
		{0x0C, "reserved"},
		{0x0D, "reserved"},
		{0x0E, "reserved"},
		{0x0F, "raw_custom"},
		{0xFF, "unknown"},
	}
	for _, tt := range tests {
		got := api.PayloadTypeName(tt.input)
		if got != tt.want {
			t.Errorf("PayloadTypeName(%#x) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestPayloadTypeFromString(t *testing.T) {
	tests := []struct {
		input string
		want  int16
	}{
		{"request", int16(meshcore.PayloadTypeReq)},
		{"req", int16(meshcore.PayloadTypeReq)},
		{"response", int16(meshcore.PayloadTypeResponse)},
		{"txt_msg", int16(meshcore.PayloadTypeTxtMsg)},
		{"txtmsg", int16(meshcore.PayloadTypeTxtMsg)},
		{"text", int16(meshcore.PayloadTypeTxtMsg)},
		{"direct", int16(meshcore.PayloadTypeTxtMsg)},
		{"acknowledgement", int16(meshcore.PayloadTypeAck)},
		{"ack", int16(meshcore.PayloadTypeAck)},
		{"advertisement", int16(meshcore.PayloadTypeAdvert)},
		{"advert", int16(meshcore.PayloadTypeAdvert)},
		{"grp_txt", int16(meshcore.PayloadTypeGrpTxt)},
		{"group_text", int16(meshcore.PayloadTypeGrpTxt)},
		{"group", int16(meshcore.PayloadTypeGrpTxt)},
		{"path", int16(meshcore.PayloadTypePath)},
		{"trace", int16(meshcore.PayloadTypeTrace)},
		{"multipart", int16(meshcore.PayloadTypeMultiPart)},
		{"multi-part", int16(meshcore.PayloadTypeMultiPart)},
		{"control", int16(meshcore.PayloadTypeControl)},
		{"raw_custom", int16(meshcore.PayloadTypeRawCustom)},
		{"raw", int16(meshcore.PayloadTypeRawCustom)},
		{"ADVERT", int16(meshcore.PayloadTypeAdvert)}, // case insensitive
		{"", -1},
		{"garbage", -1},
	}
	for _, tt := range tests {
		got := api.PayloadTypeFromString(tt.input)
		if got != tt.want {
			t.Errorf("PayloadTypeFromString(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestRouteTypeName(t *testing.T) {
	tests := []struct {
		input int16
		want  string
	}{
		{int16(meshcore.RouteTypeFlood), "FLOOD"},
		{int16(meshcore.RouteTypeDirect), "DIRECT"},
		{int16(meshcore.RouteTypeTransportFlood), "TRANSPORT_FLOOD"},
		{int16(meshcore.RouteTypeTransportDirect), "TRANSPORT_DIRECT"},
		{99, "unknown"},
	}
	for _, tt := range tests {
		got := api.RouteTypeName(tt.input)
		if got != tt.want {
			t.Errorf("RouteTypeName(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
