// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// NodeScopeEvidence combines distinct MQTT evidence without treating the advert
// default as the complete repeat list. Repeat observations cover the named window.
type NodeScopeEvidence struct {
	Since              int64       `json:"since"`
	Until              int64       `json:"until"`
	Scopes             []NodeScope `json:"scopes"`
	ReportedEmpty      bool        `json:"reportedEmpty,omitempty"`
	ConflictingReports bool        `json:"conflictingReports,omitempty"`
}

type NodeScope struct {
	Name               string `json:"name"`               // * denotes observed unscoped repeats or an explicitly reported wildcard.
	RepeatObservations int64  `json:"repeatObservations"` // accepted hearings; several observers may hear the same RF transmission
	RepeatedAt         *int64 `json:"repeatedAt,omitempty"`
	MinHashBytes       int16  `json:"minHashBytes,omitempty"` // regional path-prefix resolution, not hardware attestation
	Reported           bool   `json:"reported,omitempty"`
	ReportedAt         *int64 `json:"reportedAt,omitempty"` // omitted for legacy reports whose scope time was not recorded
	HeardAt            *int64 `json:"heardAt,omitempty"`    // receiver heard this scope; not evidence that it repeated it
}
