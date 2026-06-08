// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// IATA represents a known airport/location code used to group observers and packets.
// IATAs are auto-created on first packet arrival from that location.
// DisplayName, Lat and Lng are optional — they are set via config file override
// or remain nil if the IATA was auto-created from packet traffic.
type IATA struct {
	IATA        string   `json:"iata"`
	DisplayName *string  `json:"displayName"`
	Lat         *float64 `json:"lat"`
	Lng         *float64 `json:"lon"`
}
