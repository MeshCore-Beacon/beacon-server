// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// RegionSummary is the minimal region representation used in list responses.
type RegionSummary struct {
	ID   int    `json:"id"`
	Slug string `json:"slug"` // URL-safe identifier e.g. "western-canada"
	Name string `json:"name"`
}

// Region is the full region representation including map display hints and
// the list of IATA codes that are members of this region.
type Region struct {
	RegionSummary
	Description *string  `json:"description,omitempty"`
	CenterLat   *float64 `json:"centerLat,omitempty"` // map center latitude
	CenterLng   *float64 `json:"centerLng,omitempty"` // map center longitude
	ZoomLevel   *int     `json:"zoomLevel,omitempty"` // suggested map zoom level
	IATAs       []string `json:"iatas"`               // member IATA codes
}
