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

type MapStateFilter struct {
	IATAs    []string
	RegionID *int
}

type MapScope struct {
	IATAs    []string `json:"iatas"`
	RegionID *int     `json:"regionId,omitempty"`
}

type MapMetadata struct {
	Basemap            string `json:"basemap"`
	RoutesComplete     bool   `json:"routesComplete"`
	RoutesStatus       string `json:"routesStatus"`
	LiveDefaultEnabled bool   `json:"liveDefaultEnabled"`
}

type MapNode struct {
	ID            string   `json:"id"`
	Label         string   `json:"label"`
	Role          string   `json:"role"`
	Lat           float64  `json:"lat"`
	Lng           float64  `json:"lng"`
	FirstSeen     int64    `json:"firstSeen"`
	LastSeen      int64    `json:"lastSeen"`
	IATAsHeardIn  []string `json:"iatasHeardIn"`
	ActivityCount int64    `json:"activityCount"`
}

type MapObserver struct {
	ID               string  `json:"id"`
	Label            string  `json:"label"`
	Type             string  `json:"type"`
	IATA             string  `json:"iata"`
	Lat              float64 `json:"lat"`
	Lng              float64 `json:"lng"`
	Online           bool    `json:"online"`
	LastSeen         int64   `json:"lastSeen"`
	ObservationCount int64   `json:"observationCount"`
}

type MapRouteEndpoint struct {
	NodeID string  `json:"nodeId"`
	Label  string  `json:"label"`
	Lat    float64 `json:"lat"`
	Lng    float64 `json:"lng"`
}

type MapRoute struct {
	ID                string           `json:"id"`
	From              MapRouteEndpoint `json:"from"`
	To                MapRouteEndpoint `json:"to"`
	PacketCount       int              `json:"packetCount"`
	LastHeard         int64            `json:"lastHeard"`
	PayloadTypeNames  []string         `json:"payloadTypeNames"`
	ResolutionQuality string           `json:"resolutionQuality"`
}

type MapActivitySummary struct {
	Packets24h         int64  `json:"packets24h"`
	Observations24h    int64  `json:"observations24h"`
	ActiveObservers24h int64  `json:"activeObservers24h"`
	ActiveIATAs24h     int64  `json:"activeIatas24h"`
	LastHeardAt        *int64 `json:"lastHeardAt"`
}

type MapState struct {
	ServerTime      int64              `json:"serverTime"`
	Scope           MapScope           `json:"scope"`
	Metadata        MapMetadata        `json:"metadata"`
	Nodes           []MapNode          `json:"nodes"`
	Observers       []MapObserver      `json:"observers"`
	Routes          []MapRoute         `json:"routes"`
	ActivitySummary MapActivitySummary `json:"activitySummary"`
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
	// GetMapState returns sanitized, mappable state for the Tower map page.
	GetMapState(ctx context.Context, filter MapStateFilter) (*MapState, error)
}
