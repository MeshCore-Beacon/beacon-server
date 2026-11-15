// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"strings"
	"testing"
)

func TestInstanceName(t *testing.T) {
	cases := map[string]string{
		"":                             "Beacon",
		"   ":                          "Beacon",
		"BEACON":                       "Beacon",
		"beacon":                       "Beacon",
		" Beacon ":                     "Beacon",
		"Beacon - Ottawa":              "Beacon - Ottawa",
		"  Mesh \t  Net  ":             "Mesh Net",
		"Mesh\r\nBcc: x@y.z":           "MeshBcc: x@y.z",
		"\x00\x07\x1b":                 "Beacon",
		"Réseau Mesh":                  "Réseau Mesh",
		"BEACON Ottawa":                "BEACON Ottawa",
		"a\u2028b":                     "ab",
		strings.Repeat("x", 80):        strings.Repeat("x", 60),
		strings.Repeat("é", 80):        strings.Repeat("é", 60),
		strings.Repeat("x", 59) + " y": strings.Repeat("x", 59),
	}
	for in, want := range cases {
		if got := InstanceName(in); got != want {
			t.Errorf("InstanceName(%q) = %q, want %q", in, got, want)
		}
	}
}
