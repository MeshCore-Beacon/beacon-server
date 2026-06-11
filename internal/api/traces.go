// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// TraceTagSummary is a single trace tag with aggregate metadata.
type TraceTagSummary struct {
	TraceTag     string    `json:"traceTag"`     // hex-encoded 4-byte tag
	FirstHeardAt int64     `json:"firstHeardAt"` // epoch ms
	LastHeardAt  int64     `json:"lastHeardAt"`  // epoch ms
	PacketCount  int64     `json:"packetCount"`  // number of packets with this trace tag
	IATACount    int64     `json:"iataCount"`    // number of distinct IATAs where heard
	TraceType    string    `json:"traceType"`    // TRACE or PING
	PathHashes   []string  `json:"pathHashes"`   // hops from the most complete observation
	SNRValues    []float32 `json:"snrValues"`    // SNR per hop from the most complete observation
}

// TracePacket is a single packet within a trace series, including its
// resolved route derived from the trace path hashes.
type TracePacket struct {
	PacketHash    string        `json:"packetHash"`      // hex-encoded packet hash
	RouteType     int16         `json:"routeType"`       // numeric route type
	RouteTypeName string        `json:"routeTypeName"`   // human-readable route type
	Scope         *string       `json:"scope,omitempty"` // transport scope name, if known
	FirstHeardAt  int64         `json:"firstHeardAt"`    // epoch ms
	LastHeardAt   int64         `json:"lastHeardAt"`     // epoch ms
	RawPath       []RawHop      `json:"rawPath"`         // hops as received in the packet
	ResolvedRoute []ResolvedHop `json:"resolvedRoute"`   // hops resolved to known nodes
}

// RawHop is a single hop in a trace path as received in the packet.
type RawHop struct {
	Hash string   `json:"hash"`          // hex-encoded path hash
	SNR  *float32 `json:"snr,omitempty"` // signal-to-noise ratio in dB, if available
}

// TraceDetail is the full trace series for a given trace tag.
type TraceDetail struct {
	TraceTag string        `json:"traceTag"` // hex-encoded 4-byte tag
	Packets  []TracePacket `json:"packets"`  // all packets observed for this trace
}
