// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import "errors"

var ErrDirectoryExpired = errors.New("observer directory snapshot expired")
var ErrDirectoryBusy = errors.New("observer directory temporarily unavailable")

// ObserverDirectoryQuery is fixed when the first page creates a snapshot.
type ObserverDirectoryQuery struct {
	MatchNone bool     `json:"matchNone"`
	IATAs     []string `json:"iatas"`
	Type      string   `json:"type"`
	Broker    string   `json:"broker"`
	Status    string   `json:"status"`
	Name      string   `json:"name"`
	Scope     string   `json:"scope"`
	Sort      string   `json:"sort"`
	Since     int64    `json:"since"`
	Until     int64    `json:"until"`
	Snapshot  string   `json:"-"`
	Cursor    int64    `json:"-"`
	Limit     int32    `json:"-"`
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
	Snapshot            string                    `json:"snapshot"`
	GeneratedAt         int64                     `json:"generatedAt"`
	ExpiresAt           int64                     `json:"expiresAt"`
	WindowStart         int64                     `json:"windowStart"`
	WindowEnd           int64                     `json:"windowEnd"`
	Sort                string                    `json:"sort"`
	EffectiveSort       string                    `json:"effectiveSort"`
	Coverage            ObserverDirectoryCoverage `json:"coverage"`
	MaxObservationCount *int64                    `json:"maxObservationCount"`
	ObserverTypes       []string                  `json:"observerTypes"`
}
