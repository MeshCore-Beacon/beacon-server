// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"regexp"
	"testing"
)

func TestHigherVersion(t *testing.T) {
	for _, tc := range []struct{ a, b, want string }{
		{"", "", ""},
		{"2.0.3", "", "2.0.3"},
		{"", "2.0.3", "2.0.3"},
		{"2.0.3", "2.0.10", "2.0.10"},
		{"2.1.0", "2.0.9", "2.1.0"},
		{"10.0.0", "9.9.9", "10.0.0"},
		{"2.0.3", "2.0.3", "2.0.3"},
	} {
		if got := HigherVersion(tc.a, tc.b); got != tc.want {
			t.Errorf("HigherVersion(%q, %q) = %q, want %q", tc.a, tc.b, got, tc.want)
		}
	}
}

// Clients reject anything but plain X.Y.Z, so a typo here would lock them all out.
func TestClientFloorsAreValid(t *testing.T) {
	re := regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
	for name, v := range map[string]string{"MinAppVersion": MinAppVersion, "MinWebVersion": MinWebVersion} {
		if v != "" && !re.MatchString(v) {
			t.Errorf("%s = %q, want X.Y.Z or empty", name, v)
		}
	}
}
