// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

type topAdvertisersReader struct {
	api.Reader
	calls int64
}

func (r *topAdvertisersReader) GetStatsTopAdvertisers(_ context.Context, _ []string, _ time.Time, _ int32, sort api.AdvertiserSort) ([]api.TopAdvertiser, error) {
	r.calls++
	return []api.TopAdvertiser{{PublicKey: string(sort), AdvertCount: r.calls}}, nil
}

func (r *topAdvertisersReader) AnalyticsRevision(context.Context) (int64, error) { return 0, nil }

func TestTopAdvertisersCacheSeparatesSorts(t *testing.T) {
	c, _ := newTestClient(t)
	inner := &topAdvertisersReader{}
	reader := NewCachedReader(inner, c, CacheTTLs{Stats: time.Hour})
	since := time.UnixMilli(3600000)
	for _, sort := range []api.AdvertiserSort{api.AdvertiserSortFlood, api.AdvertiserSortDirect, api.AdvertiserSortFlood, api.AdvertiserSortDirect} {
		rows, err := reader.GetStatsTopAdvertisers(context.Background(), nil, since, 10, sort)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].PublicKey != string(sort) {
			t.Fatalf("sort %s served %+v", sort, rows)
		}
	}
	if inner.calls != 2 {
		t.Fatalf("underlying calls = %d, want 2", inner.calls)
	}
}
