// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"net/url"
	"regexp"
	"time"
)

// MaxScopeSources bounds both network refresh work and imported matching keys.
const MaxScopeSources = 16

type MeshMapperConfig struct {
	Scopes MeshMapperScopesConfig `yaml:"scopes"`
}

// MeshMapperScopesConfig augments, but never replaces, the manual scopes list.
type MeshMapperScopesConfig struct {
	Enabled         bool              `yaml:"enabled"`
	Sources         map[string]string `yaml:"sources"` // configured IATA -> published regional endpoint
	RefreshInterval duration          `yaml:"refresh_interval"`
}

func (c MeshMapperScopesConfig) Interval() time.Duration {
	if c.RefreshInterval.Duration == 0 {
		return time.Hour
	}
	return c.RefreshInterval.Duration
}

var meshMapperHost = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*\.meshmapper\.net$`)

func (c *Config) validateMeshMapper() error {
	s := c.MeshMapper.Scopes
	if !s.Enabled {
		return nil
	}
	if len(s.Sources) == 0 || len(s.Sources) > MaxScopeSources {
		return fmt.Errorf("meshmapper.scopes.sources must contain 1-%d regional endpoints", MaxScopeSources)
	}
	if s.Interval() < 5*time.Minute || s.Interval() > 24*time.Hour {
		return fmt.Errorf("meshmapper.scopes.refresh_interval must be between 5m and 24h")
	}
	configured := map[string]bool{}
	for _, region := range c.Regions {
		for _, iata := range region.IATAs {
			configured[iata] = true
		}
	}
	for iata, endpoint := range s.Sources {
		if !configured[iata] {
			return fmt.Errorf("meshmapper.scopes source %q must belong to a configured region", iata)
		}
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme != "https" || u.User != nil || !meshMapperHost.MatchString(u.Host) ||
			u.Path != "/get_scopes.php" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return fmt.Errorf("meshmapper.scopes source %q must be a published https://<region>.meshmapper.net/get_scopes.php endpoint without credentials or parameters", iata)
		}
	}
	return nil
}
