package api

// IATA represents a known airport/location code used to group observers and packets.
// DisplayName, Lat and Lng are optional — they are set via config file override
// or remain nil if the IATA was auto-created from packet traffic.
type IATA struct {
	IATA        string   `json:"iata"`
	DisplayName *string  `json:"displayName"`
	Lat         *float64 `json:"lat"`
	Lng         *float64 `json:"lon"`
}
