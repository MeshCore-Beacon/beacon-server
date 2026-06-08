// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api_test

import (
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/meshcore-go/meshcore-go"
)

func TestNodeTypeName(t *testing.T) {
	tests := []struct {
		input int16
		want  string
	}{
		{int16(meshcore.AdvertTypeChat), "companion"},
		{int16(meshcore.AdvertTypeRepeater), "repeater"},
		{int16(meshcore.AdvertTypeRoom), "room_server"},
		{int16(meshcore.AdvertTypeSensor), "sensor"},
		{99, "unknown"},
		{0, "unknown"},
	}
	for _, tt := range tests {
		got := api.NodeTypeName(tt.input)
		if got != tt.want {
			t.Errorf("NodeTypeName(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestNodeTypeFromString(t *testing.T) {
	tests := []struct {
		input string
		want  int16
	}{
		{"companion", int16(meshcore.AdvertTypeChat)},
		{"chat", int16(meshcore.AdvertTypeChat)},
		{"repeater", int16(meshcore.AdvertTypeRepeater)},
		{"room_server", int16(meshcore.AdvertTypeRoom)},
		{"roomserver", int16(meshcore.AdvertTypeRoom)},
		{"room-server", int16(meshcore.AdvertTypeRoom)},
		{"room", int16(meshcore.AdvertTypeRoom)},
		{"sensor", int16(meshcore.AdvertTypeSensor)},
		{"REPEATER", int16(meshcore.AdvertTypeRepeater)}, // case insensitive
		{"", 0},
		{"unknown", 0},
		{"garbage", 0},
	}
	for _, tt := range tests {
		got := api.NodeTypeFromString(tt.input)
		if got != tt.want {
			t.Errorf("NodeTypeFromString(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}
