// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import "time"

// Meta describes deployment settings the web client needs, such as how far back data goes.
type Meta struct {
	PacketRetentionSeconds int64 `json:"packetRetentionSeconds"`
	RouteRetentionSeconds  int64 `json:"routeRetentionSeconds"`
}

// NewMeta builds Meta from resolved retention durations.
func NewMeta(packetRetention, routeRetention time.Duration) Meta {
	return Meta{
		PacketRetentionSeconds: int64(packetRetention / time.Second),
		RouteRetentionSeconds:  int64(routeRetention / time.Second),
	}
}
