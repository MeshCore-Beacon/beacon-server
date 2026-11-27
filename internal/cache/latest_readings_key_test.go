// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

type latestReadingsReader struct {
	api.Reader
	calls int
}

func (r *latestReadingsReader) ListLatestNodeReadings(_ context.Context, iatas []string, _ time.Time) ([]api.NodeLatestReading, error) {
	r.calls++
	if len(iatas) == 0 {
		return []api.NodeLatestReading{{SampledAt: 1}}, nil
	}
	if len(iatas) == 2 {
		return []api.NodeLatestReading{{SampledAt: 2}}, nil
	}
	return []api.NodeLatestReading{}, nil
}

func TestLatestReadingsKeepsGlobalAndEmptyCodeSeparate(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "global first", true: "empty code first"}[reverse], func(t *testing.T) {
			c, _ := newTestClient(t)
			inner := &latestReadingsReader{}
			reader := NewCachedReader(inner, c, CacheTTLs{})
			filters := [][]string{nil, {""}}
			if reverse {
				filters[0], filters[1] = filters[1], filters[0]
			}
			for range 2 {
				for _, filter := range filters {
					got, err := reader.ListLatestNodeReadings(context.Background(), filter, time.Now())
					if err != nil {
						t.Fatal(err)
					}
					want := 0
					if len(filter) == 0 {
						want = 1
					}
					if len(got) != want {
						t.Errorf("filter %#v returned %d rows, want %d", filter, len(got), want)
					}
				}
			}
			if inner.calls != 2 {
				t.Errorf("underlying reads=%d, want two independent cached values", inner.calls)
			}
		})
	}
}

func TestLatestReadingsKeysPreserveFilterBoundaries(t *testing.T) {
	c, _ := newTestClient(t)
	inner := &latestReadingsReader{}
	reader := NewCachedReader(inner, c, CacheTTLs{})
	for _, tc := range []struct {
		filter      []string
		rows, calls int
	}{
		{nil, 1, 1},
		{[]string{}, 1, 1}, // nil and empty both mean global
		{[]string{"YKF", "YVR"}, 1, 2},
		{[]string{"YVR", "YKF"}, 1, 2}, // set order does not change the query
		{[]string{"YKF,YVR"}, 0, 3},    // a literal comma is not two IATA values
	} {
		got, err := reader.ListLatestNodeReadings(context.Background(), tc.filter, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != tc.rows || inner.calls != tc.calls {
			t.Errorf("filter %#v: rows=%d calls=%d; want rows=%d calls=%d", tc.filter, len(got), inner.calls, tc.rows, tc.calls)
		}
	}
}

func TestLatestReadingsDoesNotReuseLegacyCSVKeys(t *testing.T) {
	c, redis := newTestClient(t)
	// Under the CSV scheme an explicit IATA "[]" occupied the would-be JSON global key.
	redis.Set("beacon:nodes:latest-readings:[]", `[{"sampledAt":999}]`)
	inner := &latestReadingsReader{}
	reader := NewCachedReader(inner, c, CacheTTLs{})
	got, err := reader.ListLatestNodeReadings(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SampledAt != 1 || inner.calls != 1 {
		t.Fatalf("legacy entry reused: readings=%+v calls=%d", got, inner.calls)
	}
}
