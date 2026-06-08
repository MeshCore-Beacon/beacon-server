// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: agpl

package api

import "github.com/google/uuid"

// RadioPreset represents a unique radio configuration observed in a given IATA,
// aggregated from both observer status messages and node adverts.
type RadioPreset struct {
	Preset     string `json:"preset"` // "freqMhz,bwKhz,sf" e.g. "910.525,62.5,7"
	IATA       string `json:"iata"`
	SourceType string `json:"sourceType"` // "observer" or "node"
	Count      int64  `json:"count"`      // number of observers or nodes on this preset in this IATA
}

// StatsOverview is the top-level network summary for the overview endpoint.
type StatsOverview struct {
	TotalPackets      int64 `json:"totalPackets"`
	TotalObservations int64 `json:"totalObservations"`
	ActiveObservers   int64 `json:"activeObservers"`
	ActiveIATAs       int64 `json:"activeIatas"`
	WindowHours       int   `json:"windowHours"` // always 24 for now
}

// ObservationPoint is a single time-bucketed observation count for charting.
type ObservationPoint struct {
	Hour             int64  `json:"hour"` // epoch ms, start of the 1-hour bucket
	IATA             string `json:"iata"`
	ObservationCount int64  `json:"observationCount"`
	UniquePackets    int64  `json:"uniquePackets"`
	ActiveObservers  int64  `json:"activeObservers"`
}

// PayloadBreakdownItem is a single payload type with its observation count.
type PayloadBreakdownItem struct {
	PayloadType     int16  `json:"payloadType"`
	PayloadTypeName string `json:"payloadTypeName"`
	Count           int64  `json:"count"`
}

// ScopeStats represents aggregate statistics for a single transport scope.
type ScopeStats struct {
	Name          string `json:"name"`          // normalized scope name e.g. "#bc"
	PacketCount   int64  `json:"packetCount"`   // distinct packets matched to this scope
	ObserverCount int64  `json:"observerCount"` // distinct observers that forwarded packets in this scope
	NodeCount     int64  `json:"nodeCount"`     // distinct nodes with this as their default scope
}

// TopNode is a node ranked by observation count from the mv_top_nodes_by_iata materialized view.
type TopNode struct {
	NodeID           uuid.UUID `json:"nodeId"`
	NodeName         *string   `json:"nodeName,omitempty"`
	NodeType         int16     `json:"nodeType"`
	NodeTypeName     string    `json:"nodeTypeName"`
	IATA             string    `json:"iata"`
	ObservationCount int64     `json:"observationCount"`
	LastHeard        int64     `json:"lastHeard"` // epoch ms
}

// TopObserver is an observer ranked by observation count.
type TopObserver struct {
	ObserverID       uuid.UUID `json:"observerId"`
	DisplayName      *string   `json:"displayName,omitempty"`
	ObserverType     *string   `json:"observerType,omitempty"`
	IATA             string    `json:"iata"`
	ObservationCount int64     `json:"observationCount"`
}
