// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import "testing"

func TestSignalBinsAndMissingAverage(t *testing.T) {
	bins := signalBins(-30, 5, 12)
	if len(bins) != 14 || bins[0].Lower != nil || *bins[0].Upper != -30 || *bins[13].Lower != 30 || bins[13].Upper != nil {
		t.Fatalf("bounds: %+v", bins)
	}
	for i := 1; i < 13; i++ {
		if *bins[i].Upper-*bins[i].Lower != 5 || *bins[i].Lower != *bins[i-1].Upper {
			t.Fatal("non-contiguous buckets")
		}
	}
	if signalAverage(0, 0) != nil || *signalAverage(0, 1) != 0 {
		t.Fatal("zero must be distinct from no samples")
	}
}
