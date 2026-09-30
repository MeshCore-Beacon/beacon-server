// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import "testing"

func TestRecordedChannelScopeStatus(t *testing.T) {
	name := "#yow"
	for _, tc := range []struct {
		name             *string
		transport, valid bool
		want             string
	}{
		{&name, true, true, "matched"}, {nil, true, true, "unknown"},
		{nil, false, true, "unscoped"}, {nil, false, false, "unavailable"},
		{&name, false, false, "matched"},
	} {
		var transport *bool
		if tc.valid {
			transport = &tc.transport
		}
		if got := RecordedChannelScopeStatus(tc.name, transport); string(got) != tc.want {
			t.Fatalf("got %s, want %s", got, tc.want)
		}
	}
}
