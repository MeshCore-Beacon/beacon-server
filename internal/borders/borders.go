// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package borders classifies advertised repeater positions against a site's
// immutable, configured operating area. It does not change heard-in IATAs.
package borders

import (
	"fmt"
	"math"

	"github.com/meshcore-go/meshcore-go"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
)

type polygon struct {
	rings orb.Polygon
	bound orb.Bound
}

// Local owns the configured polygon union and precomputed bounds.
// A nil Local disables classification. Methods do not mutate shared state.
type Local struct{ polygons []polygon }

// New copies the polygons so the classifier can be shared across readers and
// ingest workers. Antimeridian regions must use GeoJSON's split representation.
func New(polygons []orb.Polygon) (*Local, error) {
	if len(polygons) == 0 {
		return nil, fmt.Errorf("no local border polygons configured")
	}
	local := &Local{}
	for _, poly := range polygons {
		if len(poly) == 0 {
			return nil, fmt.Errorf("border polygon has no rings")
		}
		for _, ring := range poly {
			if !ring.Closed() {
				return nil, fmt.Errorf("border ring must be closed with at least four points")
			}
			for i, p := range ring {
				if !validPosition(p.Lat(), p.Lon()) {
					return nil, fmt.Errorf("border coordinates must be finite longitude/latitude in range")
				}
				if i > 0 && math.Abs(p.Lon()-ring[i-1].Lon()) > 180 {
					return nil, fmt.Errorf("split antimeridian-crossing borders into a MultiPolygon")
				}
			}
		}
		if planar.Area(poly) <= 0 {
			return nil, fmt.Errorf("border polygon must have positive area")
		}
		copy := poly.Clone()
		local.polygons = append(local.polygons, polygon{copy, copy.Bound()})
	}
	return local, nil
}

func validPosition(lat, lng float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lng) && lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180
}

// PossiblyForeign is nil when disabled, the role is not repeater, or position
// is unknown/invalid (including the protocol's 0/0 location reset).
// All polygon boundaries, including hole edges, count as local; hole interiors
// remain outside. Location is self-reported, so this is an indication only.
func (local *Local) PossiblyForeign(role int16, lat, lng *float64) *bool {
	if local == nil || role != int16(meshcore.AdvertTypeRepeater) || lat == nil || lng == nil || !validPosition(*lat, *lng) || *lat == 0 && *lng == 0 {
		return nil
	}
	point := orb.Point{*lng, *lat}
	foreign := true
	for _, poly := range local.polygons {
		if !poly.bound.Contains(point) || !planar.RingContains(poly.rings[0], point) {
			continue
		}
		inside := true
		for _, hole := range poly.rings[1:] {
			if planar.RingContains(hole, point) && planar.DistanceFrom(orb.LineString(hole), point) != 0 {
				inside = false
				break
			}
		}
		if inside {
			foreign = false
			break
		}
	}
	return &foreign
}
