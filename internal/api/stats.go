// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import "github.com/google/uuid"

// ObserverComparison counts distinct flood packet hashes in [Since, Until).
// The three groups partition TotalPackets, the union heard by either observer.
// Counts describe retained reported receptions, not radio packet loss.
type ObserverComparison struct {
	ObserverA    uuid.UUID `json:"observerA"`
	ObserverB    uuid.UUID `json:"observerB"`
	Since        int64     `json:"since"` // inclusive, epoch milliseconds
	Until        int64     `json:"until"` // exclusive, epoch milliseconds
	TotalPackets int64     `json:"totalPackets"`
	OnlyA        int64     `json:"onlyA"`
	OnlyB        int64     `json:"onlyB"`
	Both         int64     `json:"both"`
}

// RadioPreset represents a unique radio configuration observed in a given IATA,
// aggregated from both observer status messages and node adverts.
type RadioPreset struct {
	Preset     string `json:"preset"` // "freqMhz,bwKhz,sf" e.g. "910.525,62.5,7"
	IATA       string `json:"iata"`
	SourceType string `json:"sourceType"` // "observer" or "node"
	Count      int64  `json:"count"`      // number of observers or nodes on this preset in this IATA
}

// StatsOverview is the top-level network summary for the overview endpoint.
// Covers the last 24 hours that can have been rolled; hours still missing contribute nothing.
type StatsOverview struct {
	TotalPackets      int64 `json:"totalPackets"` // distinct per hour (see StatsSeriesValues)
	TotalObservations int64 `json:"totalObservations"`
	ActiveObservers   int64 `json:"activeObservers"`
	ActiveIATAs       int64 `json:"activeIatas"`
	WindowHours       int   `json:"windowHours"` // complete rolled hours the totals cover, at most 24
	Since             int64 `json:"since"`       // epoch ms, start of the window
	Until             int64 `json:"until"`       // epoch ms, exclusive end of the window
}

// StatsSeries is hourly network activity read from the analytics rollups. Only complete
// hours carry values; the summary combines them (see docs/historical-stats.md).
type StatsSeries struct {
	Since            int64             `json:"since"`            // effective start, epoch ms on a UTC hour
	Until            int64             `json:"until"`            // effective exclusive end, epoch ms on a UTC hour
	Revision         int64             `json:"revision"`         // changes whenever rolled hours, their status or coverage change
	EarliestComplete *int64            `json:"earliestComplete"` // first complete hour held, epoch ms; null if none
	CompleteHours    int               `json:"completeHours"`    // hours contributing to summary
	Hours            []StatsSeriesHour `json:"hours"`
	Summary          StatsSeriesValues `json:"summary"`
}

// StatsSeriesHour is one UTC hour of a StatsSeries.
type StatsSeriesHour struct {
	Hour   int64              `json:"hour"`                                    // epoch ms, start of the hour
	Status string             `json:"status" enums:"complete,partial,missing"` // partial: raw rows deleted before it was rolled
	Values *StatsSeriesValues `json:"values"`                                  // null unless complete
}

// StatsSeriesValues are the card metrics for an hour or a whole series. Packet counts are
// distinct per hour, so a packet heard across an hour boundary counts in both hours.
type StatsSeriesValues struct {
	Observations    int64   `json:"observations"`
	UniquePackets   int64   `json:"uniquePackets"`
	ActiveObservers int64   `json:"activeObservers"` // distinct across the hour or window
	ActiveIATAs     int64   `json:"activeIatas"`     // distinct across the hour or window
	ScopedPackets   int64   `json:"scopedPackets"`
	ActiveScopes    int64   `json:"activeScopes"` // distinct across the hour or window
	MaxPathEntries  int32   `json:"maxPathEntries"`
	SNRSum          float64 `json:"snrSum"` // average = snrSum / snrSamples
	SNRSamples      int64   `json:"snrSamples"`
	RSSISum         float64 `json:"rssiSum"` // average = rssiSum / rssiSamples
	RSSISamples     int64   `json:"rssiSamples"`
}

// ObservationPoint is a single time-bucketed observation count for charting.
// Distinct counts don't sum across IATAs, so use /stats/series for those.
type ObservationPoint struct {
	Hour             int64  `json:"hour"` // epoch ms, start of the 1-hour bucket
	IATA             string `json:"iata"`
	ObservationCount int64  `json:"observationCount"`
}

// PayloadBreakdownItem is a single payload type with its observation count.
type PayloadBreakdownItem struct {
	PayloadType     int16  `json:"payloadType"`
	PayloadTypeName string `json:"payloadTypeName"`
	Count           int64  `json:"count"`
}

// ScopeStats represents aggregate statistics for a single transport scope.
type ScopeStats struct {
	Name          string      `json:"name"`          // normalized scope name e.g. "#bc"
	PacketCount   int64       `json:"packetCount"`   // distinct packets matched to this scope
	ObserverCount int64       `json:"observerCount"` // distinct observers that forwarded packets in this scope
	NodeCount     int64       `json:"nodeCount"`     // distinct nodes with this as their default scope
	Hourly        []ScopeHour `json:"hourly"`        // packetCount by hour, oldest first; hours with none omitted
}

// ScopeHour is one UTC hour of a scope's activity; a scope's hours' packets sum to its packetCount.
type ScopeHour struct {
	Hour      int64 `json:"hour"` // epoch ms, start of the UTC hour
	Packets   int64 `json:"packets"`
	Observers int64 `json:"observers"` // distinct observers that heard the scope's packets that hour
	Nodes     int64 `json:"nodes"`     // distinct nodes whose adverts were heard in the scope that hour
}

// TopNode is a node ranked by how often its adverts were heard in the window.
type TopNode struct {
	NodeID           *uuid.UUID `json:"nodeId"`    // null once the node row has been deleted
	PublicKey        string     `json:"publicKey"` // hex
	NodeName         *string    `json:"nodeName,omitempty"`
	NodeType         int16      `json:"nodeType"`
	NodeTypeName     string     `json:"nodeTypeName"`
	IATA             string     `json:"iata"`
	ObservationCount int64      `json:"observationCount"`
	LastHeard        int64      `json:"lastHeard"` // epoch ms
}

// TopObserver is an observer ranked by observation count.
type TopObserver struct {
	ObserverID       uuid.UUID `json:"observerId"`
	DisplayName      *string   `json:"displayName,omitempty"`
	ObserverType     *string   `json:"observerType,omitempty"`
	IATA             string    `json:"iata"`
	ObservationCount int64     `json:"observationCount"`
}

// TopAdvertiser is a node ranked by distinct ADVERT packet count within the requested
// window. Count is per-advert, not per-hearing -- see GetStatsTopAdvertisers.
type TopAdvertiser struct {
	NodeID       *uuid.UUID `json:"nodeId"`    // null once the node row has been deleted
	PublicKey    string     `json:"publicKey"` // hex
	NodeName     *string    `json:"nodeName,omitempty"`
	NodeType     int16      `json:"nodeType"`
	NodeTypeName string     `json:"nodeTypeName"`
	IATA         string     `json:"iata"`
	AdvertCount  int64      `json:"advertCount"`
	// FloodAdvertCount/DirectAdvertCount split AdvertCount by how the advert was routed:
	// flood = route type 0 (transport_flood) or 1 (flood), broadcast with no known path;
	// direct = route type 2 (direct) or 3 (transport_direct), routed along a known path.
	// FloodAdvertCount + DirectAdvertCount == AdvertCount.
	FloodAdvertCount  int64 `json:"floodAdvertCount"`
	DirectAdvertCount int64 `json:"directAdvertCount"`
	LastHeard         int64 `json:"lastHeard"` // epoch ms
}

// TopTalker is a companion name ranked by decrypted channel message count within the
// requested window. Grouped by sender name as decrypted from the message itself, not by
// node identity -- see GetStatsTopTalkers.
type TopTalker struct {
	SenderName   string `json:"senderName"`
	MessageCount int64  `json:"messageCount"`
	LastSent     int64  `json:"lastSent"` // epoch ms
}

// NodeTypeCount shows the count of nodes of a given type with the type name
type NodeTypeCount struct {
	NodeType     int16  `json:"nodeType"`
	NodeTypeName string `json:"nodeTypeName"`
	Count        int64  `json:"count"`
}

// ClockDriftEntry is a repeater or room server whose most recent advert-derived clock drift
// exceeds the configured threshold (nodes.clock_drift_threshold, default 5m) -- see
// GetStatsClockDrift. Ordered worst-drift-first. ClockDriftSeconds/ClockCheckedAt mirror the
// same-named fields on Node; unlike Node this list only ever contains out-of-sync nodes, so
// there's no ClockOutOfSync bool here -- being in the list already means true.
type ClockDriftEntry struct {
	NodeID            uuid.UUID  `json:"nodeId"`
	NodeName          *string    `json:"nodeName,omitempty"`
	NodeType          int16      `json:"nodeType"`
	NodeTypeName      string     `json:"nodeTypeName"`
	ClockDriftSeconds int        `json:"clockDriftSeconds"` // signed; +ve = device ahead of server
	ClockCheckedAt    int64      `json:"clockCheckedAt"`    // epoch ms
	IATAs             []NodeIATA `json:"iatas,omitempty"`   // IATAs this node has been heard in
}
