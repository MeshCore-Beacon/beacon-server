// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package api

import (
	"strings"
	"testing"
	"time"
)

func TestRouteEvidenceCursor(t *testing.T) {
	c := RouteEvidenceCursor{IATA: "YOW", PathKey: "f097439148601d9f3291c474f82fa64c", Since: time.UnixMilli(1000).UTC(), Until: time.UnixMilli(3000).UTC(), HeardAt: time.UnixMicro(2000123).UTC(), ID: 9223372036854775806}
	got, err := ParseRouteEvidenceCursor(c.String())
	if err != nil || *got != c {
		t.Fatalf("cursor round-trip: %+v %v", got, err)
	}
	if got.HeardAt.Nanosecond() != 123000 {
		t.Fatal("lost microsecond precision")
	}
	for _, bad := range []string{"", strings.Repeat("x", 300), "v2:YOW:x:1:2:3:4", strings.Replace(c.String(), "YOW", "TOOLONG", 1), strings.Replace(c.String(), c.PathKey, "bad", 1), strings.Replace(c.String(), "2000123", "4000000", 1), strings.TrimSuffix(c.String(), "9223372036854775806") + "0"} {
		if _, err := ParseRouteEvidenceCursor(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}
