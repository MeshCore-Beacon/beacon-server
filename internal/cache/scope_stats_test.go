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

func (r *scopeStatsReader) GetScopeStats(context.Context, []string, time.Time) ([]api.ScopeStats, error) {
	r.calls++
	return []api.ScopeStats{{Name: "#test", PacketCount: r.calls}}, nil
}

func (r *scopeStatsReader) AnalyticsRevision(context.Context) (int64, error) { return 0, nil }

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
		rows, err := reader.GetScopeStats(context.Background(), tc.iatas, time.Time{})
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

func TestDefaultWindowKeyFollowsTheHour(t *testing.T) {
	c, _ := newTestClient(t)
	inner := &scopeStatsReader{}
	cr := NewCachedReader(inner, c, CacheTTLs{Stats: time.Hour}).(*CachedReader)
	clock := time.Date(2026, 10, 2, 10, 30, 0, 0, time.UTC)
	cr.now = func() time.Time { return clock }
	get := func(since time.Time) int64 {
		t.Helper()
		rows, err := cr.GetScopeStats(context.Background(), nil, since)
		if err != nil {
			t.Fatal(err)
		}
		return rows[0].PacketCount
	}
	if get(time.Time{}) != 1 || get(time.Time{}) != 1 {
		t.Fatal("default window not cached within the hour")
	}
	clock = clock.Add(35 * time.Minute)
	if get(time.Time{}) != 2 {
		t.Fatal("default window served from the previous hour")
	}
	// An explicit since at the current hour is a different window.
	if get(clock.Truncate(time.Hour)) != 3 {
		t.Fatal("explicit since collided with the default window")
	}

}
