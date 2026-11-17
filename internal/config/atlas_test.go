// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import "testing"

func TestAtlasConfigResolve(t *testing.T) {
	if r, err := (AtlasConfig{}).Resolve(); err != nil || r.Limit != 20 {
		t.Fatalf("default = %+v, %v; want limit 20", r, err)
	}
	for _, n := range []int{1, 50, 100} {
		n := n
		if r, err := (AtlasConfig{Limit: &n}).Resolve(); err != nil || r.Limit != n {
			t.Fatalf("limit %d = %+v, %v", n, r, err)
		}
	}
	for _, n := range []int{0, -1, 101} {
		n := n
		if _, err := (AtlasConfig{Limit: &n}).Resolve(); err == nil {
			t.Fatalf("limit %d: want an error", n)
		}
	}
}
