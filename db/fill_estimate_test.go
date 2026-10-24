// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"testing"
	"time"
)

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
		{1_000_200, 1_000_300, 100}, // above the histogram: new since ANALYZE, one row an id
		{1_000_150, 1_000_250, 175}, // 125 inside it and 50 above
		{-5, 0, 0},                  // below it
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

// On ids like production's, a few old rows far below a dense live range, an ETA in rows follows the work left
// where one in ids counts the empty ids as work.
func TestIDHistogramETAFollowsRows(t *testing.T) {
	// 100 rows spread over ids (0, 770M], then 10M dense rows in (770M, 780M].
	bounds := []int64{1, 770_000_000}
	for id := int64(770_100_000); id <= 780_000_000; id += 100_000 {
		bounds = append(bounds, id)
	}
	h := idHistogram{bounds: bounds, rows: float64(len(bounds)-1) * 100_000}
	writer, pos := int64(780_000_000), int64(775_000_000) // half the dense rows filled in an hour
	var p FillProgress
	start := time.Unix(0, 0)
	p.now = func() time.Time { return start }
	p.Start(h.rowsOrIDs(writer, writer))
	p.now = func() time.Time { return start.Add(time.Hour) }
	eta, ok := p.ETA(h.rowsOrIDs(pos, writer), h.rowsOrIDs(0, pos))
	if !ok || eta < 55*time.Minute || eta > 65*time.Minute {
		t.Fatalf("ETA in rows %v, %v; want about an hour", eta, ok)
	}
	var ids FillProgress
	ids.now = func() time.Time { return start }
	ids.Start(0)
	ids.now = func() time.Time { return start.Add(time.Hour) }
	if eta, _ := ids.ETA(writer-pos, pos); eta < 100*time.Hour {
		t.Fatalf("ETA in ids %v; the test's ids should make it about 155 h", eta)
	}
}
