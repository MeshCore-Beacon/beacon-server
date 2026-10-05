// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

type ObserverDirectoryQuery struct {
	MatchNone bool
	IATAs     []string
	Type      string
	Broker    string
	Status    string
	Name      string
	Scope     string
	Sort      string
	Since     int64
	Until     int64
	Cursor    int64
	Limit     int32
}

type ObserverDirectoryItem struct {
	ObserverSummary
	ObservationCount *int64 `json:"observationCount"`
}

type ObserverDirectoryCoverage struct {
	Status        string `json:"status"`
	ExpectedHours int    `json:"expectedHours"`
	CompleteHours int    `json:"completeHours"`
	PartialHours  int    `json:"partialHours"`
	MissingHours  int    `json:"missingHours"`
}

type ObserverDirectory struct {
	Page[ObserverDirectoryItem]
	GeneratedAt         int64                     `json:"generatedAt"`
	WindowStart         int64                     `json:"windowStart"`
	WindowEnd           int64                     `json:"windowEnd"`
	Sort                string                    `json:"sort"`
	EffectiveSort       string                    `json:"effectiveSort"`
	Coverage            ObserverDirectoryCoverage `json:"coverage"`
	MaxObservationCount *int64                    `json:"maxObservationCount"`
	ObserverTypes       []string                  `json:"observerTypes"`
}
