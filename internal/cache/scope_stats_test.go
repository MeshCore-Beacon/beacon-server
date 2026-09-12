// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

type scopeStatsReader struct {
	api.Reader
	calls int64
}

func (r *scopeStatsReader) GetScopeStats(context.Context, []string) ([]api.ScopeStats, error) {
	r.calls++
	return []api.ScopeStats{{Name: "#test", PacketCount: r.calls}}, nil
}

func TestScopeStatsCacheSeparatesIATAs(t *testing.T) {
	c, _ := newTestClient(t)
	inner := &scopeStatsReader{}
	reader := NewCachedReader(inner, c, CacheTTLs{Reference: time.Minute})
	for _, tc := range []struct {
		iatas []string
		want  int64
	}{
		{nil, 1},
		{[]string{"YVR"}, 2},
		{[]string{"YYJ"}, 3},
		{[]string{"YYJ", "YVR"}, 4},
		{[]string{"YVR", "YYJ", "YVR"}, 4},
		{[]string{"YVR"}, 2},
		{[]string{}, 1},
		{[]string{"ZZZ"}, 5},
		{[]string{""}, 6},
		{nil, 1},
	} {
		before := slices.Clone(tc.iatas)
		rows, err := reader.GetScopeStats(context.Background(), tc.iatas)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].PacketCount != tc.want {
			t.Fatalf("IATAs %v: got %+v, want cached count %d", tc.iatas, rows, tc.want)
		}
		if !slices.Equal(before, tc.iatas) {
			t.Fatalf("caller IATAs mutated: %v -> %v", before, tc.iatas)
		}
	}
	if inner.calls != 6 {
		t.Fatalf("underlying calls = %d, want 6", inner.calls)
	}
}
