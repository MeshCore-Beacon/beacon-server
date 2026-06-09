// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// TraceTagSummary is a single trace tag with aggregate metadata.
type TraceTagSummary struct {
	TraceTag     string `json:"traceTag"`     // hex-encoded 4-byte tag
	FirstHeardAt int64  `json:"firstHeardAt"` // epoch ms
	LastHeardAt  int64  `json:"lastHeardAt"`  // epoch ms
	PacketCount  int64  `json:"packetCount"`  // number of packets with this trace tag
	IATACount    int64  `json:"iataCount"`    // number of distinct IATAs where heard
}

// TracePacket is a single packet within a trace series, including its
// resolved route derived from the trace path hashes.
type TracePacket struct {
	PacketHash    string        `json:"packetHash"`
	RouteType     int16         `json:"routeType"`
	RouteTypeName string        `json:"routeTypeName"`
	Scope         *string       `json:"scope,omitempty"`
	FirstHeardAt  int64         `json:"firstHeardAt"` // epoch ms
	LastHeardAt   int64         `json:"lastHeardAt"`  // epoch ms
	RawPath       []RawHop      `json:"rawPath"`
	ResolvedRoute []ResolvedHop `json:"resolvedRoute"`
}

type RawHop struct {
	Hash string   `json:"hash"` // hex-encoded path hash
	SNR  *float32 `json:"snr,omitempty"`
}

// TraceDetail is the full trace series for a given trace tag.
type TraceDetail struct {
	TraceTag string        `json:"traceTag"` // hex-encoded 4-byte tag
	Packets  []TracePacket `json:"packets"`
}
