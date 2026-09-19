// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package borders

import (
	"math"
	"testing"

	"github.com/paulmach/orb"
)

func TestPossiblyForeign(t *testing.T) {
	outer := orb.Ring{{10, 10}, {20, 10}, {20, 20}, {10, 20}, {10, 10}}
	hole := orb.Ring{{12, 12}, {14, 12}, {14, 14}, {12, 14}, {12, 12}}
	other := orb.Ring{{30, 10}, {40, 10}, {40, 20}, {30, 20}, {30, 10}}
	local, err := New([]orb.Polygon{{outer, hole}, {other}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		role     int16
		lat, lng float64
		want     *bool
	}{
		{"inside", 2, 15, 15, new(false)},
		{"outside", 2, 25, 25, new(true)},
		{"second polygon", 2, 15, 35, new(false)},
		{"hole", 2, 13, 13, new(true)},
		{"exterior edge", 2, 15, 10, new(false)},
		{"hole edge", 2, 13, 12, new(false)},
		{"vertex", 2, 10, 10, new(false)},
		{"companion", 1, 25, 25, nil},
		{"room", 3, 25, 25, nil},
		{"sensor", 4, 25, 25, nil},
		{"zero reset", 2, 0, 0, nil},
		{"equator is valid", 2, 0, 25, new(true)},
		{"prime meridian is valid", 2, 25, 0, new(true)},
		{"invalid latitude", 2, 91, 25, nil},
		{"invalid longitude", 2, 25, -181, nil},
		{"NaN", 2, math.NaN(), 25, nil},
		{"infinite", 2, 25, math.Inf(1), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := local.PossiblyForeign(tc.role, &tc.lat, &tc.lng)
			if (got == nil) != (tc.want == nil) || got != nil && *got != *tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
	if local.PossiblyForeign(2, nil, new(15.0)) != nil || local.PossiblyForeign(2, new(15.0), nil) != nil {
		t.Fatal("missing coordinate classified")
	}
	var disabled *Local
	if disabled.PossiblyForeign(2, new(25.0), new(25.0)) != nil {
		t.Fatal("disabled classification")
	}
	// The shared startup classifier owns its geometry; callers cannot mutate it.
	outer[0] = orb.Point{99, 99}
	if *local.PossiblyForeign(2, new(15.0), new(15.0)) {
		t.Fatal("input mutation changed classifier")
	}
}

func TestLocalBordersValidation(t *testing.T) {
	for _, polys := range [][]orb.Polygon{
		nil, {{}}, {{{{0, 0}, {1, 0}, {1, 1}}}},
		{{{{0, 0}, {0, 0}, {0, 0}, {0, 0}}}},
		{{{{179, 10}, {-179, 10}, {-179, 20}, {179, 20}, {179, 10}}}},
	} {
		if _, err := New(polys); err == nil {
			t.Fatalf("accepted invalid/uncut geometry: %v", polys)
		}
	}
	cut := []orb.Polygon{
		{{{179, 10}, {180, 10}, {180, 20}, {179, 20}, {179, 10}}},
		{{{-180, 10}, {-179, 10}, {-179, 20}, {-180, 20}, {-180, 10}}},
	}
	local, err := New(cut)
	if err != nil {
		t.Fatal(err)
	}
	for _, lon := range []float64{179.5, -179.5, 180, -180} {
		if *local.PossiblyForeign(2, new(15.0), &lon) {
			t.Fatalf("dateline island excluded: %v", lon)
		}
	}
	if !*local.PossiblyForeign(2, new(15.0), new(0.0)) {
		t.Fatal("uncut global region inferred")
	}
}

func BenchmarkPossiblyForeign(b *testing.B) {
	ring := make(orb.Ring, 10001)
	for i := 0; i < 10000; i++ {
		a := 2 * math.Pi * float64(i) / 10000
		ring[i] = orb.Point{20 + math.Cos(a), 50 + math.Sin(a)}
	}
	ring[10000] = ring[0]
	local, err := New([]orb.Polygon{{ring}})
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		lat, lng float64
	}{{"inside_10k_vertices", 50, 20}, {"outside_bounds_10k_vertices", 0, 1}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				local.PossiblyForeign(2, &tc.lat, &tc.lng)
			}
		})
	}
}
