// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// NodeScopeEvidence combines distinct MQTT evidence without treating the advert
// default as the complete repeat list. Repeat observations cover the named window.
type NodeScopeEvidence struct {
	Since              int64           `json:"since"`
	Until              int64           `json:"until"`
	Scopes             []NodeScope     `json:"scopes"`
	Hourly             []NodeScopeHour `json:"hourly,omitempty"` // detail only; absent hours have no evidence
	ReportedEmpty      bool            `json:"reportedEmpty,omitempty"`
	ConflictingReports bool            `json:"conflictingReports,omitempty"`
}

type NodeScope struct {
	Name                 string `json:"name"`               // * is unscoped/wildcard; ? is unnamed scoped traffic, never a guessed scope name.
	RepeatObservations   int64  `json:"repeatObservations"` // accepted hearings; several observers may hear the same RF transmission
	RepeatObservations1h *int64 `json:"repeatObservations1h,omitempty"`
	RepeatObservations6h *int64 `json:"repeatObservations6h,omitempty"`
	FirstRepeatedAt      *int64 `json:"firstRepeatedAt,omitempty"`
	UniqueSamples        int64  `json:"uniqueSamples"`
	UniqueSince          *int64 `json:"uniqueSince,omitempty"`
	RepeatedAt           *int64 `json:"repeatedAt,omitempty"`
	MinHashBytes         int16  `json:"minHashBytes,omitempty"` // regional path-prefix resolution, not hardware attestation
	Reported             bool   `json:"reported,omitempty"`
	ReportedAt           *int64 `json:"reportedAt,omitempty"` // omitted for legacy reports whose scope time was not recorded
	HeardAt              *int64 `json:"heardAt,omitempty"`    // receiver heard this scope; not evidence that it repeated it
}

// NodeScopeHour is observed data only. Missing buckets must not be filled as zero traffic.
type NodeScopeHour struct {
	Name          string `json:"name"`
	Hour          int64  `json:"hour"`
	Reports       int64  `json:"reports"`
	UniqueSamples int64  `json:"uniqueSamples"`
	FirstSeen     int64  `json:"firstSeen"`
	LastSeen      int64  `json:"lastSeen"`
}
