// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package config loads the Beacon configuration file and seeds the database
// with regions, IATA overrides, and channel keys on startup.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level structure of the Beacon config file.
type Config struct {
	IATAs       map[string]IATAConfig `yaml:"iatas"`
	Regions     []RegionConfig        `yaml:"regions"`
	ChannelKeys ChannelKeysConfig     `yaml:"channel_keys"`
	Telemetry   TelemetryConfig       `yaml:"telemetry"`
	WebSocket   WebSocketConfig       `yaml:"websocket"`
	Packets     PacketsConfig         `yaml:"packets"`
	Routes      RoutesConfig          `yaml:"routes"`
	Ingest      IngestFilterConfig    `yaml:"ingest"`
	Scopes      []ScopeConfig         `yaml:"scopes"`
	Cache       CacheConfig           `yaml:"cache"`
	CORS        CORSConfig            `yaml:"cors"`
	Background  BackgroundConfig      `yaml:"background"`
	Presence    PresenceConfig        `yaml:"presence"`
	Nodes       NodesConfig           `yaml:"nodes"`
	Observers   ObserversConfig       `yaml:"observers"`
}

// ResolvedConfig holds all runtime configuration with defaults applied.
type ResolvedConfig struct {
	TelemetryResolution  time.Duration
	TelemetryRetention   time.Duration
	PacketRetention      time.Duration
	RouteRetention       time.Duration
	RouteGrace           time.Duration
	RouteMinObservations int
	MaxConnsPerIP        int
	ViewRefreshInterval  time.Duration
	ReconfirmInterval    time.Duration
	CleanupInterval      time.Duration

	PresenceFlushInterval time.Duration
	PresencePacketTTL     time.Duration

	// ClockDriftThreshold is the |device clock - server clock| magnitude, measured from a
	// node's ADVERT timestamp, above which the node API reports clockOutOfSync=true for
	// that node. Only meaningful for repeaters/room servers (nodeType 2/3).
	ClockDriftThreshold time.Duration

	// NodeStaleThreshold and NodeDeleteAfter mirror ClockDriftThreshold's "0 means unset,
	// resolve to a default" pattern -- see NodesConfig.
	NodeStaleThreshold  time.Duration
	NodeDeleteAfter     time.Duration
	ObserverDeleteAfter time.Duration
}

// PresenceConfig controls coalescing of presence bookkeeping writes
// (observer last_seen, observer_brokers, packet last_heard_at bumps).
type PresenceConfig struct {
	// FlushInterval is how often coalesced bumps are flushed to Postgres.
	// Defaults to 30s if not set.
	FlushInterval duration `yaml:"flush_interval"`

	// PacketTTL is how long a packet hash with no re-observations stays
	// coalesced before the next observation writes through again.
	// Defaults to 30s if not set.
	PacketTTL duration `yaml:"packet_ttl"`
}

// BackgroundConfig controls the intervals for background maintenance tasks.
type BackgroundConfig struct {
	// ViewRefresh is how often materialized views are refreshed.
	// Defaults to 1h if not set.
	ViewRefresh duration `yaml:"view_refresh"`

	// Reconfirm prunes stale and ambiguous resolved paths and neigbors.
	Reconfirm duration `yaml:"reconfirm"`

	// Cleanup is how often old telemetry and packet rows are pruned.
	// Defaults to 1h if not set.
	Cleanup duration `yaml:"cleanup"`
}

// CORSConfig controls Cross-Origin Resource Sharing behaviour.
// If omitted, Beacon defaults to allowing all origins, which is appropriate
// for a public read-only API. Operators exposing write endpoints should
// restrict AllowedOrigins to known frontends.
type CORSConfig struct {
	// AllowedOrigins is the list of origins permitted to make cross-origin
	// requests. Use ["*"] to allow all origins (default if omitted).
	AllowedOrigins []string `yaml:"allowed_origins"`

	// AllowedMethods is the list of HTTP methods allowed in CORS requests.
	// Defaults to [GET, HEAD, OPTIONS] if omitted.
	AllowedMethods []string `yaml:"allowed_methods"`

	// AllowedHeaders is the list of request headers allowed in CORS requests.
	// Defaults to [Accept, Authorization, Content-Type] if omitted.
	AllowedHeaders []string `yaml:"allowed_headers"`

	// AllowCredentials indicates whether the request can include user
	// credentials (cookies, HTTP authentication). Defaults to false.
	AllowCredentials bool `yaml:"allow_credentials"`

	// MaxAge is the number of seconds the browser may cache a preflight
	// response. Defaults to 300 if omitted.
	MaxAge int `yaml:"max_age"`
}

// CacheConfig controls Redis caching behaviour.
// If Addr is not set (via REDIS_ADDR env var), caching is disabled entirely
// and Beacon falls back to querying PostgreSQL directly for all reads.
type CacheConfig struct {
	// TTL is the default cache entry lifetime applied to all categories
	// that do not have an explicit override in TTLs. Defaults to 1h if not set.
	TTL duration `yaml:"ttl"`

	// TTLs holds optional per-category TTL overrides. Any category left
	// at zero will fall back to TTL.
	TTLs CacheTTLsConfig `yaml:"ttls"`
}

// CacheTTLsConfig holds per-category TTL overrides for the cache layer.
// Each field is optional — omit it in config to inherit the global TTL.
type CacheTTLsConfig struct {
	// Stats controls the TTL for aggregated network statistics endpoints
	// (overview, observations, payload breakdown, top nodes/observers, radio presets, scope stats).
	// These are backed by materialized views refreshed hourly, so values under 1m are rarely useful.
	Stats duration `yaml:"stats"`

	// Reference controls the TTL for mostly-static reference data
	// (IATAs, regions, scopes). These change only when new observers
	// arrive or config is reseeded.
	Reference duration `yaml:"reference"`

	// Nodes controls the TTL for individual node detail responses.
	// Acts as a safety-net expiry alongside explicit invalidation on upsert.
	Nodes duration `yaml:"nodes"`

	// Observers controls the TTL for individual observer detail responses.
	// Acts as a safety-net expiry alongside explicit invalidation on upsert.
	Observers duration `yaml:"observers"`
}

// ScopeConfig defines a regional transport scope.
// Name can be provided with or without the # or $ prefix.
// Beacon normalizes plain names by prepending #.
type ScopeConfig struct {
	Name string `yaml:"name"` // e.g. "bc", "#west", "$private"
}

// TelemetryConfig controls observer telemetry storage behaviour.
type TelemetryConfig struct {
	// Retention is how long telemetry rows are kept before the cleanup job removes them.
	// Defaults to 672h (4 weeks) if not set.
	Retention duration `yaml:"retention"`

	// Resolution is how frequently a telemetry snapshot is stored per observer.
	// Status messages arriving within the same resolution window are deduplicated.
	// Defaults to 1h if not set.
	Resolution duration `yaml:"resolution"`
}

// WebSocketConfig controls WebSocket connection behaviour.
// Settings here apply to the /ws endpoint only.
type WebSocketConfig struct {
	// MaxConnectionsPerIP is the maximum number of concurrent WebSocket
	// connections allowed from a single IP address. Defaults to 5 if not set.
	MaxConnectionsPerIP int `yaml:"max_connections_per_ip"`
}

// PacketsConfig controls packet retention behaviour.
type PacketsConfig struct {
	// Retention is how long packet and observation rows are kept.
	// Defaults to 720h (30 days) if not set.
	Retention duration `yaml:"retention"`
}

// RoutesConfig controls known-route retention behaviour.
type RoutesConfig struct {
	// Retention is how long a route is kept after it was last observed.
	// Defaults to 336h (14 days) if not set.
	Retention duration `yaml:"retention"`
	// Grace is how long a route observed fewer than MinObservations times is
	// kept. Defaults to 168h (7 days) if not set.
	Grace duration `yaml:"grace"`
	// MinObservations is the observation count below which Grace applies
	// instead of Retention. Defaults to 3 if not set.
	MinObservations int `yaml:"min_observations"`
}

// NodesConfig controls node-derived signal thresholds.
type NodesConfig struct {
	// ClockDriftThreshold is the |device clock - server clock| magnitude, measured from a
	// repeater/room server's ADVERT timestamp, above which the node API reports
	// clockOutOfSync=true for that node. Defaults to 5m if not set.
	ClockDriftThreshold duration `yaml:"clock_drift_threshold"`
	// StaleThreshold is how long since a node's last_seen before the node API reports
	// stale=true for it. Defaults to 24h if not set.
	StaleThreshold duration `yaml:"stale_threshold"`
	// DeleteAfter is how long since a node's last_seen before the cleanup job deletes the
	// node entirely. Defaults to the same 30-day default as packets.retention if not set --
	// independently configurable from it, just the same starting point.
	DeleteAfter duration `yaml:"delete_after"`
}

// ObserversConfig controls optional observer age-out.
type ObserversConfig struct {
	// DeleteAfter is how long an observer must be unseen before it can be deleted.
	// Omitted or nonpositive disables age-out. Retained packet observations,
	// telemetry and ownership records always protect the observer from deletion.
	DeleteAfter duration `yaml:"delete_after"`
}

// duration is a wrapper around time.Duration that supports YAML unmarshalling
// from human-readable strings like "24h", "7d", "30d".
type duration struct {
	time.Duration
}

func (d *duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

// ChannelKeysConfig holds both hashtag-derived and explicit channel keys.
// Hashtag keys are derived automatically: secret = SHA256("#tag")[:16],
// channel_hash = SHA256(secret)[0]. Explicit keys are provided as hex strings
// keyed by the channel hash hex (e.g. "11" for 0x11).
type ChannelKeysConfig struct {
	// Hashtags is a list of hashtag names (without the # prefix).
	// Beacon derives the PSK and channel hash automatically.
	Hashtags []string `yaml:"hashtags"`

	// Keys maps channel hash hex → explicit key config.
	Keys map[string]ExplicitKeyConfig `yaml:"keys"`
}

// ExplicitKeyConfig holds an explicit channel key and optional display name.
type ExplicitKeyConfig struct {
	Key  string `yaml:"key"`  // hex-encoded key bytes
	Name string `yaml:"name"` // optional display name
}

// IATAConfig holds optional overrides for a known IATA code.
type IATAConfig struct {
	Name string   `yaml:"name"`
	Lat  *float64 `yaml:"lat"`
	Lng  *float64 `yaml:"lng"`
	// BorderFile is a path to a GeoJSON Feature file (Polygon or MultiPolygon geometry) for
	// this IATA's region border map. Relative paths are resolved against the directory
	// containing the main config file (see Load). Validated and bbox-computed at seed time --
	// see border.go.
	BorderFile string `yaml:"borderFile"`
}

// RegionConfig defines a super-region and its member IATAs.
type RegionConfig struct {
	Slug         string   `yaml:"slug"`
	Name         string   `yaml:"name"`
	Description  string   `yaml:"description"`
	DisplayOrder int      `yaml:"display_order"`
	CenterLat    *float64 `yaml:"center_lat"`
	CenterLng    *float64 `yaml:"center_lng"`
	ZoomLevel    *int     `yaml:"zoom_level"`
	IATAs        []string `yaml:"iatas"`
}

// IngestFilterConfig restricts which packets Beacon stores based on the
// observer's IATA geographic location. Both filters are optional — if neither
// is set all IATAs are accepted. If both are set an IATA passes if it matches
// either (OR semantics).
//
// Country codes are ISO 3166-1 alpha-2 (e.g. "CA", "US").
// Continent codes are two-letter OurAirports codes: AF, AN, AS, EU, NA, OC, SA.
type IngestFilterConfig struct {
	// AllowCountries is a list of ISO 3166-1 alpha-2 country codes to accept.
	// Packets from observers in other countries are dropped at ingest.
	AllowCountries []string `yaml:"allow_countries"`

	// AllowContinents is a list of continent codes to accept.
	// Packets from observers in other continents are dropped at ingest.
	AllowContinents []string `yaml:"allow_continents"`
}

// Load reads and parses the config file at path.
// Returns an empty Config (not an error) if the file does not exist,
// so Beacon starts cleanly without a config file.
func Load(path string) (*Config, error) {
	cfg := &Config{}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	configDir := filepath.Dir(path)
	for iata, details := range cfg.IATAs {
		if details.BorderFile != "" && !filepath.IsAbs(details.BorderFile) {
			details.BorderFile = filepath.Join(configDir, details.BorderFile)
			cfg.IATAs[iata] = details
		}
	}
	return cfg, nil
}

// Resolve returns a ResolvedConfig with defaults applied for any zero values.
func Resolve(cfg *Config) ResolvedConfig {
	r := ResolvedConfig{
		TelemetryResolution:  cfg.Telemetry.Resolution.Duration,
		TelemetryRetention:   cfg.Telemetry.Retention.Duration,
		PacketRetention:      cfg.Packets.Retention.Duration,
		RouteRetention:       cfg.Routes.Retention.Duration,
		RouteGrace:           cfg.Routes.Grace.Duration,
		RouteMinObservations: cfg.Routes.MinObservations,
		MaxConnsPerIP:        cfg.WebSocket.MaxConnectionsPerIP,
		ViewRefreshInterval:  cfg.Background.ViewRefresh.Duration,
		ReconfirmInterval:    cfg.Background.Reconfirm.Duration,
		CleanupInterval:      cfg.Background.Cleanup.Duration,

		PresenceFlushInterval: cfg.Presence.FlushInterval.Duration,
		PresencePacketTTL:     cfg.Presence.PacketTTL.Duration,

		ClockDriftThreshold: cfg.Nodes.ClockDriftThreshold.Duration,
		NodeStaleThreshold:  cfg.Nodes.StaleThreshold.Duration,
		NodeDeleteAfter:     cfg.Nodes.DeleteAfter.Duration,
		ObserverDeleteAfter: cfg.Observers.DeleteAfter.Duration,
	}
	if r.TelemetryResolution == 0 {
		r.TelemetryResolution = time.Hour
	}
	if r.TelemetryRetention == 0 {
		r.TelemetryRetention = 28 * 24 * time.Hour
	}
	if r.PacketRetention == 0 {
		r.PacketRetention = 30 * 24 * time.Hour
	}
	if r.RouteRetention == 0 {
		r.RouteRetention = 14 * 24 * time.Hour
	}
	if r.RouteGrace == 0 {
		r.RouteGrace = 7 * 24 * time.Hour
	}
	if r.RouteMinObservations == 0 {
		r.RouteMinObservations = 3
	}
	if r.MaxConnsPerIP == 0 {
		r.MaxConnsPerIP = 5
	}
	if r.ViewRefreshInterval == 0 {
		r.ViewRefreshInterval = time.Hour
	}
	if r.ReconfirmInterval == 0 {
		r.ReconfirmInterval = time.Hour
	}
	if r.CleanupInterval == 0 {
		r.CleanupInterval = time.Hour
	}
	if r.PresenceFlushInterval == 0 {
		r.PresenceFlushInterval = 30 * time.Second
	}
	if r.PresencePacketTTL == 0 {
		r.PresencePacketTTL = 30 * time.Second
	}
	if r.ClockDriftThreshold == 0 {
		r.ClockDriftThreshold = 5 * time.Minute
	}
	if r.NodeStaleThreshold == 0 {
		r.NodeStaleThreshold = 24 * time.Hour
	}
	if r.NodeDeleteAfter == 0 {
		// Same default as packets.retention (30 days) -- independently configurable, just
		// the same starting point, not tied to whatever PacketRetention resolves to.
		r.NodeDeleteAfter = 30 * 24 * time.Hour
	}
	return r
}

func (r ResolvedConfig) String() string {
	return fmt.Sprintf(
		"telemetryResolution=%s telemetryRetention=%s packetRetention=%s routeRetention=%s routeGrace=%s routeMinObs=%d maxConnsPerIP=%d viewRefresh=%s reconfirm=%s cleanup=%s presenceFlush=%s presencePacketTTL=%s clockDriftThreshold=%s nodeStaleThreshold=%s nodeDeleteAfter=%s observerDeleteAfter=%s",
		r.TelemetryResolution, r.TelemetryRetention, r.PacketRetention, r.RouteRetention, r.RouteGrace, r.RouteMinObservations,
		r.MaxConnsPerIP, r.ViewRefreshInterval, r.ReconfirmInterval, r.CleanupInterval,
		r.PresenceFlushInterval, r.PresencePacketTTL, r.ClockDriftThreshold,
		r.NodeStaleThreshold, r.NodeDeleteAfter, r.ObserverDeleteAfter,
	)
}
