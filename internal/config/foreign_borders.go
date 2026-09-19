// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"os"
	"sort"

	"github.com/MeshCore-Beacon/beacon-server/internal/borders"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

// LoadLocalBorders builds one startup-only classifier from configured border
// files. IATA airport coordinates and auto-discovered IATAs are not boundaries.
func LoadLocalBorders(cfg *Config) (*borders.Local, error) {
	if !cfg.Nodes.MarkForeign {
		return nil, nil
	}
	var codes []string
	for code, details := range cfg.IATAs {
		if details.BorderFile != "" {
			codes = append(codes, code)
		}
	}
	sort.Strings(codes)
	var polygons []orb.Polygon
	for _, code := range codes {
		raw, err := os.ReadFile(cfg.IATAs[code].BorderFile)
		if err != nil {
			return nil, fmt.Errorf("nodes.mark_foreign: reading border for %s: %w", code, err)
		}
		validated, err := ValidateBorder(raw)
		if err != nil {
			return nil, fmt.Errorf("nodes.mark_foreign: invalid border for %s: %w", code, err)
		}
		feature, err := geojson.UnmarshalFeature(validated)
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
