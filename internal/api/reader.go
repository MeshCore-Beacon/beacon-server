package api

import (
	"context"
)

type RegionSummary struct {
	ID   int    `json:"id"`
	Slug string `json:"slug"`
	Name string `json:"name"`
}

type Region struct {
	RegionSummary
	Description *string  `json:"description,omitempty"`
	CenterLat   *float64 `json:"centerLat,omitempty"`
	CenterLng   *float64 `json:"centerLng,omitempty"`
	ZoomLevel   *int     `json:"zoomLevel,omitempty"`
	IATAs       []string `json:"iatas"`
}

// IATA represents a known airport/location code used to group observers and packets.
// DisplayName, Lat and Lng are optional — they are set via config file override
// or remain nil if the IATA was auto-created from packet traffic.
type IATA struct {
	IATA        string   `json:"iata"`
	DisplayName *string  `json:"displayName"`
	Lat         *float64 `json:"lat"`
	Lng         *float64 `json:"lon"`
}

type Reader interface {
	// ListIATAs returns all known IATA codes with display name and coordinates.
	// IATAs are auto-created on first packet arrival from that location.
	ListIATAs(ctx context.Context) ([]IATA, error)
	// GetIATA returns a single IATA code by its 3-letter identifier.
	// Returns nil, error if the IATA code is not found.
	GetIATA(ctx context.Context, iata string) (*IATA, error)
	// ListRegions returns a summary list of all regions ordered by display_order then name.
	// Use GetRegion for full detail including associated IATAs.
	ListRegions(ctx context.Context) ([]RegionSummary, error)
	// GetRegion returns full detail for a single region including its associated IATA codes.
	// Returns nil, pgx.ErrNoRows if the region is not found.
	GetRegion(ctx context.Context, regionID int32) (*Region, error)
}
