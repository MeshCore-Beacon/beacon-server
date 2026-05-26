// Package api defines the response types and read interface for the Tower REST API.
package api

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ObserverSummary is the minimal observer representation used in list responses.
type ObserverSummary struct {
	ID           uuid.UUID `json:"id"`
	DisplayName  *string   `json:"displayName,omitempty"`  // friendly name from /status messages
	ObserverType *string   `json:"observerType,omitempty"` // e.g. "meshcoretomqtt", "meshcoreha"
	IATA         string    `json:"iata"`                   // most recently heard IATA
	Status       string    `json:"status"`                 // "online" or "offline" derived from last_status_at
}

// Observer is the full observer representation including radio config,
// telemetry, broker memberships and raw status metadata.
type Observer struct {
	ObserverSummary
	PublicKey        string          `json:"publicKey"` // hex-encoded public key
	SoftwareVersion  *string         `json:"softwareVersion,omitempty"`
	HardwareModel    *string         `json:"hardwareModel,omitempty"`
	FirmwareVersion  *string         `json:"firmwareVersion,omitempty"`
	FirmwareBuild    *string         `json:"firmwareBuild,omitempty"`
	RadioFreqMHz     *float32        `json:"radioFreqMhz,omitempty"` // MHz e.g. 910.525
	RadioSF          *int16          `json:"radioSf,omitempty"`      // LoRa spreading factor
	RadioBWKHz       *float32        `json:"radioBwKhz,omitempty"`   // bandwidth in kHz
	RadioCR          *int16          `json:"radioCr,omitempty"`      // coding rate denominator
	BatteryLevel     *float32        `json:"batteryLevel,omitempty"` // volts, nil if mains powered
	UptimeSeconds    *int64          `json:"uptimeSeconds,omitempty"`
	StatusMetadata   json.RawMessage `json:"statusMetadata,omitempty"` // raw /status JSON payload
	LastStatusAt     *string         `json:"lastStatusAt,omitempty"`   // RFC3339
	FirstSeen        string          `json:"firstSeen"`                // RFC3339
	LastSeen         string          `json:"lastSeen"`                 // RFC3339
	ObservationCount int64           `json:"observationCount"`
	Brokers          []string        `json:"brokers"` // broker names this observer has been seen on
}

// ChannelMessage represents a single decrypted channel message.
// Only messages for channels with a known key are stored and returned.
type ChannelMessage struct {
	ID          int64  `json:"id"`
	PacketHash  string `json:"packetHash"`  // hex-encoded packet hash for correlation with packet events
	ChannelHash string `json:"channelHash"` // hex-encoded single-byte channel hash
	SenderName  string `json:"senderName"`  // display name from the decrypted payload
	Content     string `json:"content"`     // decrypted message text
	SentAt      string `json:"sentAt"`      // RFC3339 timestamp from the packet payload
}

// ChannelSummary is the minimal channel representation used in list responses.
type ChannelSummary struct {
	ID          int     `json:"id"`
	Name        *string `json:"name,omitempty"` // display name, nil if not set
	ChannelHash string  `json:"channelHash"`    // hex-encoded single-byte hash
	LastSeen    string  `json:"lastSeen"`       // ISO 8601 timestamp
	IsHashtag   bool    `json:"isHashtag"`      // true if derived from a hashtag PSK
	KeyKnown    bool    `json:"keyKnown"`       // true if Tower has a decryption key
}

// Channel is the full channel representation including decryption metadata.
// KeyFingerprint is only populated for hashtag channels since their keys are
// publicly derivable from the tag name.
type Channel struct {
	ChannelSummary
	Hashtag        *string `json:"hashtag,omitempty"`        // tag name without # prefix
	KeyFingerprint *string `json:"keyFingerprint,omitempty"` // first 8 bytes of SHA256(key), hex-encoded
	MessageCount   int64   `json:"messageCount"`
}

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
	// ListChannels returns a summary list of all known channels ordered by last seen.
	// Includes both hashtag-derived and explicit key channels.
	// Channels with unknown keys are included with KeyKnown=false.
	// Pass nil hash to skip hash filtering. Pass empty string iata to return all channels.
	ListChannels(ctx context.Context, limit int32, hash []byte, iata string) ([]ChannelSummary, error)
	// GetChannel returns full detail for a single channel by its integer ID.
	// Returns nil, pgx.ErrNoRows if the channel is not found.
	GetChannel(ctx context.Context, channelID int32) (*Channel, error)
	// ListChannelMessages returns paginated messages for a channel identified by its integer ID.
	// Used by the /channels/{id}/messages endpoint.
	// Pass a zero time.Time for since to return all messages up to limit.
	// Pass empty string iata to return messages from all IATAs.
	ListChannelMessages(ctx context.Context, channelID *int32, since time.Time, limit int32, iata string) ([]ChannelMessage, error)
	// ListChannelMessagesByHash returns paginated messages for all channels matching the given hash.
	// Used by the /messages?hash= endpoint. May return messages from multiple channels
	// if the hash collides across different keys.
	// Pass a zero time.Time for since to return all messages up to limit.
	// Pass empty string iata to return messages from all IATAs.
	ListChannelMessagesByHash(ctx context.Context, hash []byte, since time.Time, limit int32, iata string) ([]ChannelMessage, error)
	// ListObservers returns a summary list of observers with optional filters.
	// All filter params are optional — pass empty string to skip a filter.
	// status is "online" or "offline" derived from last_status_at recency.
	ListObservers(ctx context.Context, iata, observerType, broker, status string) ([]ObserverSummary, error)
	// GetObserver returns full detail for a single observer by UUID.
	// Returns nil, pgx.ErrNoRows if the observer is not found.
	GetObserver(ctx context.Context, observerID uuid.UUID) (*Observer, error)
}
