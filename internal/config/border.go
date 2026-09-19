// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"encoding/json"
	"fmt"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

// ValidateBorder parses and validates raw as a GeoJSON Feature suitable for a region border
// map: well-formed Feature, geometry Polygon or MultiPolygon, every coordinate within
// lon [-180,180] / lat [-90,90], every ring closed (first point == last point, 4+ points --
// see orb.Ring.Closed). On success, returns the Feature re-marshaled to JSON with its bbox
// computed and set, so reads never need to recompute it (db/migrations/011_add_iata_border.sql).
//
// Coordinate order: GeoJSON packs coordinates [lon, lat], not [lat, lon]. A border authored
// with the axes swapped is still well-formed JSON and its numbers can still individually fall
// within the ranges checked here -- there is no way to detect a swapped axis order from the
// numbers alone. It lands in the wrong place on the map silently. Author borders as
// [lon, lat] and spot-check new ones visually; this function cannot catch that mistake.
func ValidateBorder(raw []byte) (json.RawMessage, error) {
	feat, err := geojson.UnmarshalFeature(raw)
	if err != nil {
		return nil, fmt.Errorf("not a well-formed GeoJSON Feature: %w", err)
	}
	if feat.Type != "Feature" {
		return nil, fmt.Errorf(`top-level "type" must be "Feature", got %q`, feat.Type)
	}
	if feat.Geometry == nil {
		return nil, fmt.Errorf("Feature must have a Polygon or MultiPolygon geometry")
	}
	switch geom := feat.Geometry.(type) {
	case orb.Polygon:
		if err := validateBorderPolygon(geom); err != nil {
			return nil, err
		}
	case orb.MultiPolygon:
		if len(geom) == 0 {
			return nil, fmt.Errorf("MultiPolygon has no polygons")
		}
		for i, poly := range geom {
			if err := validateBorderPolygon(poly); err != nil {
				return nil, fmt.Errorf("polygon %d: %w", i, err)
			}
		}
	default:
		return nil, fmt.Errorf("geometry must be Polygon or MultiPolygon, got %s", feat.Geometry.GeoJSONType())
	}
	// Computed server-side so reads (GET /iatas/{iata}/border) never need to walk every
	// vertex just to fit a map view -- see RFC 7946 section 5.
	feat.BBox = geojson.NewBBox(feat.Geometry.Bound())
	out, err := json.Marshal(feat)
	if err != nil {
		return nil, fmt.Errorf("re-marshaling validated border: %w", err)
	}
	return out, nil
}

func validateBorderPolygon(poly orb.Polygon) error {
	if len(poly) == 0 {
		return fmt.Errorf("polygon has no rings")
	}
	for i, ring := range poly {
		if !ring.Closed() {
			return fmt.Errorf("ring %d is not closed (needs 4+ points, first and last must match)", i)
		}
		for _, pt := range ring {
			lon, lat := pt.Lon(), pt.Lat()
			if lon < -180 || lon > 180 {
				return fmt.Errorf("ring %d: longitude %g out of range [-180,180]", i, lon)
			}
			if lat < -90 || lat > 90 {
				return fmt.Errorf("ring %d: latitude %g out of range [-90,90]", i, lat)
			}
		}
	}
	return nil
}
