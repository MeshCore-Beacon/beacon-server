// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"

	"github.com/MeshCore-Beacon/beacon-server/internal/borders"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

func (c *Config) hasBorderFile() bool {
	for _, details := range c.IATAs {
		if details.BorderFile != "" {
			return true
		}
	}
	return false
}

// LoadLocalBorders reads the validated border files used for foreign marking,
// keyed by IATA. IATA airport coordinates are not boundaries. Nil when disabled.
func LoadLocalBorders(cfg *Config) (map[string]json.RawMessage, error) {
	if !cfg.Nodes.MarkForeign {
		return nil, nil
	}
	files := map[string]json.RawMessage{}
	for code, details := range cfg.IATAs {
		if details.BorderFile == "" {
			continue
		}
		raw, err := os.ReadFile(details.BorderFile)
		if err != nil {
			return nil, fmt.Errorf("nodes.mark_foreign: reading border for %s: %w", code, err)
		}
		validated, err := ValidateBorder(raw)
		if err != nil {
			return nil, fmt.Errorf("nodes.mark_foreign: invalid border for %s: %w", code, err)
		}
		files[code] = validated
	}
	// Fail at startup rather than on the first rebuild.
	if _, err := BuildLocalBorders(files, nil); err != nil {
		return nil, err
	}
	return files, nil
}

// BuildLocalBorders unions validated borders; an imported boundary replaces
// the same IATA's file. Nil when there are none.
func BuildLocalBorders(files, imported map[string]json.RawMessage) (*borders.Local, error) {
	merged := maps.Clone(files)
	if merged == nil {
		merged = map[string]json.RawMessage{}
	}
	maps.Copy(merged, imported)
	if len(merged) == 0 {
		return nil, nil
	}
	var polygons []orb.Polygon
	for _, code := range slices.Sorted(maps.Keys(merged)) {
		feature, err := geojson.UnmarshalFeature(merged[code])
		if err != nil {
			return nil, fmt.Errorf("nodes.mark_foreign: decoding border for %s: %w", code, err)
		}
		switch geometry := feature.Geometry.(type) {
		case orb.Polygon:
			polygons = append(polygons, geometry)
		case orb.MultiPolygon:
			polygons = append(polygons, geometry...)
		}
	}
	local, err := borders.New(polygons)
	if err != nil {
		return nil, fmt.Errorf("nodes.mark_foreign: %w", err)
	}
	return local, nil
}
