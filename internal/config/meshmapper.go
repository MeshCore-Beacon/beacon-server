// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"log/slog"
	"time"
)

// MeshMapper rate-limits get_zones/get_geojson to one request per target per
// 23.5h and get_scopes to one per region per 55m; minimums round up.
const (
	MinZonesRefresh    = 24 * time.Hour
	MinScopesRefresh   = time.Hour
	MinChannelsRefresh = 24 * time.Hour // get_channels.php: one per region per 23.5h
)

type MeshMapperConfig struct {
	Scopes   MeshMapperScopesConfig   `yaml:"scopes"`
	Zones    MeshMapperZonesConfig    `yaml:"zones"`
	Channels MeshMapperChannelsConfig `yaml:"channels"`
}

// MeshMapperChannelsConfig imports each known IATA's public hashtag channels.
type MeshMapperChannelsConfig struct {
	Enabled         bool     `yaml:"enabled"`
	RefreshInterval duration `yaml:"refresh_interval"`
}

func (c MeshMapperChannelsConfig) Interval() time.Duration {
	if c.RefreshInterval.Duration == 0 {
		return MinChannelsRefresh
	}
	return c.RefreshInterval.Duration
}

// MeshMapperZonesConfig imports each known IATA's published boundary, overriding borderFile.
type MeshMapperZonesConfig struct {
	Enabled         bool     `yaml:"enabled"`
	ImportGroups    bool     `yaml:"import_groups"` // zone groups become regions
	RefreshInterval duration `yaml:"refresh_interval"`
}

func (c MeshMapperZonesConfig) Interval() time.Duration {
	if c.RefreshInterval.Duration == 0 {
		return 24 * time.Hour
	}
	return c.RefreshInterval.Duration
}

// MeshMapperScopesConfig augments, but never replaces, the manual scopes list.
// Sources are discovered per known IATA from the zone list.
type MeshMapperScopesConfig struct {
	Enabled         bool              `yaml:"enabled"`
	Sources         map[string]string `yaml:"sources"` // deprecated: ignored, kept to warn
	RefreshInterval duration          `yaml:"refresh_interval"`
}

func (c MeshMapperScopesConfig) Interval() time.Duration {
	if c.RefreshInterval.Duration == 0 {
		return time.Hour
	}
	return c.RefreshInterval.Duration
}

func (c *Config) validateMeshMapper() error {
	if z := c.MeshMapper.Zones; z.ImportGroups && !z.Enabled {
		return fmt.Errorf("meshmapper.zones.import_groups requires meshmapper.zones.enabled")
	}
	if z := c.MeshMapper.Zones; z.Enabled {
		if z.Interval() < MinZonesRefresh || z.Interval() > 7*24*time.Hour {
			return fmt.Errorf("meshmapper.zones.refresh_interval must be between 24h and 168h")
		}
	}
	if ch := c.MeshMapper.Channels; ch.Enabled && (ch.Interval() < MinChannelsRefresh || ch.Interval() > 7*24*time.Hour) {
		return fmt.Errorf("meshmapper.channels.refresh_interval must be between 24h and 168h")
	}
	s := c.MeshMapper.Scopes
	if !s.Enabled {
		return nil
	}
	if s.Interval() < MinScopesRefresh || s.Interval() > 24*time.Hour {
		return fmt.Errorf("meshmapper.scopes.refresh_interval must be between 1h and 24h")
	}
	if len(s.Sources) > 0 {
		slog.Warn("meshmapper.scopes.sources is ignored; sources are discovered from the MeshMapper zone list", "component", "config")
	}
	return nil
}
