// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import "testing"

func TestIDHistogramEstimate(t *testing.T) {
	// 1,000 rows in 4 equal buckets: 250 in (0, 100], 250 in (100, 1,000,000] (a sparse old range), 500 in the dense
	// top ids.
	h := idHistogram{bounds: []int64{1, 100, 1_000_000, 1_000_100, 1_000_200}, rows: 1000}
	for _, c := range []struct {
		after, through, want int64
	}{
		{0, 1_000_200, 1000},
		{1_000_000, 1_000_200, 500},
		{100, 1_000_000, 250},
		{1_000_100, 1_000_150, 125},
		{500_000, 1_000_000, 125},
		{1_000_200, 2_000_000, 0}, // above the histogram
		{-5, 0, 0},                // below it
		{50, 50, 0},
	} {
		if got, ok := h.estimate(c.after, c.through); !ok || got != c.want {
			t.Errorf("estimate(%d, %d) = %d, %v; want %d", c.after, c.through, got, ok, c.want)
		}
	}
	if _, ok := (idHistogram{}).estimate(0, 10); ok {
		t.Error("an empty histogram estimated rows")
	}
	if got := (idHistogram{}).rowsOrIDs(10, 25); got != 15 {
		t.Errorf("rowsOrIDs without statistics = %d, want the id count 15", got)
	}
}
