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

type signalStatsReader struct {
	api.Reader
	calls int64
}

func (r *signalStatsReader) GetSignalStats(context.Context, time.Time, time.Time, []string) (*api.SignalStats, error) {
	r.calls++
	return &api.SignalStats{Receptions: r.calls}, nil
}

func TestSignalCacheSeparatesWindowAndRegion(t *testing.T) {
	c, mr := newTestClient(t)
	inner := &signalStatsReader{}
	r := NewCachedReader(inner, c, CacheTTLs{Stats: time.Minute})
	for _, tc := range []struct {
		iatas              []string
		since, until, want int64
	}{
		{nil, 0, 10, 1}, {[]string{}, 0, 10, 1}, {[]string{""}, 0, 10, 2}, {[]string{"YVR"}, 0, 10, 3},
		{[]string{"YVR", "YYJ"}, 0, 10, 4}, {[]string{"YYJ", "YVR", "YYJ"}, 0, 10, 4},
		{nil, 1, 10, 5}, {nil, 0, 11, 6}, {nil, 0, 10, 1},
	} {
		before := slices.Clone(tc.iatas)
		got, err := r.GetSignalStats(context.Background(), time.UnixMilli(tc.since), time.UnixMilli(tc.until), tc.iatas)
		if err != nil || got.Receptions != tc.want || !slices.Equal(before, tc.iatas) {
			t.Fatalf("%+v: %+v %v", tc, got, err)
		}
	}
	mr.FastForward(time.Minute)
	got, err := r.GetSignalStats(context.Background(), time.UnixMilli(0), time.UnixMilli(10), nil)
	if err != nil || got.Receptions != 7 {
		t.Fatalf("expiry: %+v %v", got, err)
	}
}
