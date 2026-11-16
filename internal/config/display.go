// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"strings"
)

// DisplayConfig sets the units clients show for this instance.
type DisplayConfig struct {
	TemperatureUnit string `yaml:"temperature_unit"` // c (default) or f
	DistanceUnit    string `yaml:"distance_unit"`    // metric (default) or imperial
}

// ResolvedDisplay is DisplayConfig with defaults applied and values normalized.
type ResolvedDisplay struct {
	TemperatureUnit string
	DistanceUnit    string
}

// Resolve applies defaults, lowercases and trims values, and rejects unknown units.
func (d DisplayConfig) Resolve() (ResolvedDisplay, error) {
	r := ResolvedDisplay{
		TemperatureUnit: strings.ToLower(strings.TrimSpace(d.TemperatureUnit)),
		DistanceUnit:    strings.ToLower(strings.TrimSpace(d.DistanceUnit)),
	}
	if r.TemperatureUnit == "" {
		r.TemperatureUnit = "c"
	}
	if r.DistanceUnit == "" {
		r.DistanceUnit = "metric"
	}
	if r.TemperatureUnit != "c" && r.TemperatureUnit != "f" {
		return ResolvedDisplay{}, fmt.Errorf("display.temperature_unit must be c or f, got %q", d.TemperatureUnit)
	}
	if r.DistanceUnit != "metric" && r.DistanceUnit != "imperial" {
		return ResolvedDisplay{}, fmt.Errorf("display.distance_unit must be metric or imperial, got %q", d.DistanceUnit)
	}
	return r, nil
}
