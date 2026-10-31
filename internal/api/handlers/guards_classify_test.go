// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http/httptest"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/guards"
)

// TestGuardsClassifyReadsQueryLikeHandlers pins the guard classifier to the way
// the packet and directory handlers read since/until and the region filters,
// including repeated and blank values.
func TestGuardsClassifyReadsQueryLikeHandlers(t *testing.T) {
	for _, raw := range []string{
		"", "since=1", "since=", "since=&since=1", "since=1&since=", "until=2", "until=&until=2",
		"iata=YOW", "iata=", "iata=&iata=YOW", "iata=%20", "iatas=,", "iatas=,%20,", "iatas=YVR,", "iatas=&iatas=YVR",
		"iatas=,&iata=YOW", "iatas=&iata=YOW", "region=bc", "region=&region=bc", "regionId=3", "regionId=&regionId=3",
		"hashPrefix=ab", "iatas=YVR&since=1",
	} {
		r := httptest.NewRequest("GET", "/api/v1/packets?"+raw, nil)
		q := r.URL.Query()
		region := len(parseIATAs(r)) > 0 || q.Get("regionId") != "" || q.Get("region") != ""
		want := guards.PacketsGlobal
		switch {
		case q.Get("since") != "" || q.Get("until") != "":
			want = guards.PacketsWindow
		case region:
			want = guards.PacketsRegion
		}
		if f, _ := guards.Classify(r); f != want {
			t.Errorf("packets?%s: Classify = %s, want %s", raw, f, want)
		}

		d := httptest.NewRequest("GET", "/api/v1/observers/directory?"+raw, nil)
		wantClass := guards.Aggregation
		if region {
			wantClass = guards.List
		}
		if _, c := guards.Classify(d); c != wantClass {
			t.Errorf("observers/directory?%s: class = %s, want %s", raw, c, wantClass)
		}
	}
}
