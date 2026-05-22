package api

import "context"

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
}
