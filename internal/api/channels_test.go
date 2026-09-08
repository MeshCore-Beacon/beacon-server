// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"strings"
	"testing"
	"time"
)

func TestChannelCursorRoundTrip(t *testing.T) {
	for _, at := range []time.Time{
		time.Date(2026, 9, 8, 12, 0, 0, 123456000, time.FixedZone("offset", 3600)),
		time.UnixMicro(0), time.UnixMicro(-1),
		time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC),
	} {
		want := ChannelCursor{LastSeen: at, ID: 2147483647}
		got, err := ParseChannelCursor(want.String())
		if err != nil || got.ID != want.ID || !got.LastSeen.Equal(want.LastSeen) {
			t.Fatalf("round trip lost timestamp precision or ID: %v", err)
		}
	}
}

func TestChannelCursorInvalid(t *testing.T) {
	for _, raw := range []string{"", "1", "v2:1:1", "v1:1", "v1:1:1:1", "v1:x:1", "v1:1.5:1",
		"v1:1:0", "v1:1:-1", "v1:1:2147483648", "v1:9223372036854775808:1",
		"v1:-9223372036854775808:1", "v1:9223372036854775807:1", strings.Repeat("x", 65)} {
		if _, err := ParseChannelCursor(raw); err == nil {
			t.Errorf("accepted invalid cursor %q", raw)
		}
	}
}
