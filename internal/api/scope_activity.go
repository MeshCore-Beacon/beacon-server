// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import "github.com/google/uuid"

// ScopeActivity describes receiving-area evidence, never transmitter country or border direction.
type ScopeActivity struct {
	Since     int64                   `json:"since"`
	Until     int64                   `json:"until"`
	FirstSeen *int64                  `json:"firstSeen,omitempty"`
	LastSeen  *int64                  `json:"lastSeen,omitempty"`
	Truncated bool                    `json:"truncated"`
	Scopes    []ScopeActivityRow      `json:"scopes"`
	Observers []ScopeObserverActivity `json:"observers"`
	Countries []ScopeCountryActivity  `json:"countries"`
}
type ScopeActivityRow struct {
	Name               string              `json:"name"`
	ReceiverReports    int64               `json:"receiverReports"`
	UniqueSamples      int64               `json:"uniqueSamples"` // packet/path/receiving-area/hour; not unique RF transmissions
	RepeatReports      int64               `json:"repeatReports"` // repeater appearances, not packets
	ActiveRepeaters    int64               `json:"activeRepeaters"`
	ReceivingObservers int64               `json:"receivingObservers"`
	UnresolvedHops     int64               `json:"unresolvedHops"`
	AmbiguousHops      int64               `json:"ambiguousHops"`
	Hourly             []ScopeActivityHour `json:"hourly"`
}
type ScopeActivityHour struct {
	Hour            int64 `json:"hour"`
	ReceiverReports int64 `json:"receiverReports"`
	UniqueSamples   int64 `json:"uniqueSamples"`
	RepeatReports   int64 `json:"repeatReports"`
}
type ScopeObserverActivity struct {
	ObserverID uuid.UUID `json:"observerId"`
	Name       string    `json:"name"`
	IATA       string    `json:"iata"`
	Country    string    `json:"country"` // country of the declared MQTT receiving area
	Scope      string    `json:"scope"`
	Reports    int64     `json:"reports"`
	LastSeen   int64     `json:"lastSeen"`
}
type ScopeCountryActivity struct {
	Country       string `json:"country"`
	Scope         string `json:"scope"`
	Reports       int64  `json:"reports"`
	UniqueSamples int64  `json:"uniqueSamples"`
	Observers     int64  `json:"observers"`
}
