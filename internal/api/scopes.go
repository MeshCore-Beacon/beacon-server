// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// ScopeSummary is the minimal scope representation used in filtered list responses.
type ScopeSummary struct {
	Name          string `json:"name"`
	ObserverCount int64  `json:"observerCount"`
	NodeCount     int64  `json:"nodeCount"`
	IATACount     int64  `json:"iataCount"`
}

// ScopeDetail is the full scope representation including packet count and IATA list.
type ScopeDetail struct {
	Name          string   `json:"name"`
	PacketCount   int64    `json:"packetCount"`
	ObserverCount int64    `json:"observerCount"`
	NodeCount     int64    `json:"nodeCount"`
	IATACount     int64    `json:"iataCount"`
	IATAs         []string `json:"iatas"`
}
