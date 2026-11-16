// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
)

func TestListReadsKeepEmptyRegionFiltered(t *testing.T) {
	var got []string
	var calls int
	record := func(iatas []string) { got = slices.Clone(iatas); calls++ }
	region := func(empty bool) *api.Region {
		if empty {
			return &api.Region{IATAs: []string{}}
		}
		return &api.Region{IATAs: []string{"YVR", "YYJ"}}
	}
	reader := stubReader{
		getRegion: func(_ context.Context, id int32) (*api.Region, error) {
			if id == 1 {
				return region(true), nil
			}
			if id == 2 {
				return region(false), nil
			}
			return nil, pgx.ErrNoRows
		},
		getRegionBySlug: func(_ context.Context, slug string) (*api.Region, error) {
			if slug == "empty" {
				return region(true), nil
			}
			if slug == "west" {
				return region(false), nil
			}
			return nil, pgx.ErrNoRows
		},
		listNodes: func(_ context.Context, _ int16, iatas []string, _, _ *bool, _ []byte, _, _, _ string, _ int64, _ *api.NodeCursor, _ int32, _ bool) (api.NodePage, error) {
			record(iatas)
			return api.NodePage{}, nil
		},
		listNodesSorted: func(_ context.Context, _ int16, iatas []string, _, _ *bool, _ []byte, _, _, _ string, _ api.NodeSort, _ *api.NodeSortCursor, _ int32, _ bool) (api.NodePage, error) {
			record(iatas)
			return api.NodePage{}, nil
		},
		listLatestNodeReadings: func(_ context.Context, iatas []string, _ time.Time) ([]api.NodeLatestReading, error) {
			record(iatas)
			return nil, nil
		},
		listObservers: func(_ context.Context, iatas []string, _, _, _, _, _ string, _ int64, _ *api.ObserverCursor, _ int32) (api.ObserverPage, error) {
			record(iatas)
			return api.ObserverPage{}, nil
		},
		listPackets: func(_ context.Context, _, _ []int16, iatas []string, _ []string, _, _ time.Time, _ int64, _ *api.PacketCursor, _ int32) (api.PacketPage, error) {
			record(iatas)
			return api.PacketPage{}, nil
		},
		searchPacketsByHashPrefix: func(_ context.Context, _, _ []byte, _, _ []int16, iatas []string, _ []string, _, _ time.Time, _ int64, _ int32) (api.Page[api.PacketSummary], error) {
			record(iatas)
			return api.Page[api.PacketSummary]{}, nil
		},
		listPacketsAfterID: func(_ context.Context, _ int64, _, _ int16, iatas []string, _ string, _ int32) ([]api.PacketSummary, error) {
			record(iatas)
			return nil, nil
		},
		listMessagesAfterID: func(_ context.Context, _ int64, iatas []string, _ string, _ int32) ([]api.ChannelMessage, error) {
			record(iatas)
			return nil, nil
		},
		listTraceTags: func(_ context.Context, iatas []string, _, _ string, _, _, _ time.Time, _ string, _ int32) ([]api.TraceTagSummary, error) {
			record(iatas)
			return nil, nil
		},
	}
	for _, endpoint := range []struct {
		path    string
		handler func(api.Reader) http.HandlerFunc
	}{
		{"/nodes", listNodes},
		{"/nodes?sort=name", listNodes},
		{"/nodes/telemetry/latest", listLatestNodeReadings},
		{"/observers", listObservers},
		{"/packets", listPackets},
		{"/packets?hashPrefix=abcd", listPackets},
		{"/packets/backfill?afterObservationId=1", listPacketsBackfill},
		{"/messages/backfill?afterId=1", listMessagesBackfill},
		{"/traces", listTraceTags},
	} {
		t.Run(endpoint.path, func(t *testing.T) {
			for _, tc := range []struct {
				query  string
				want   []string
				status int
			}{
				{"", nil, 200},
				{"iata=ykf", []string{"YKF"}, 200},
				{"region=empty", []string{emptyRegionIATA}, 200},
				{"regionId=1", []string{emptyRegionIATA}, 200},
				{"region=empty&iata=ykf", []string{"YKF"}, 200},
				{"region=west", []string{"YVR", "YYJ"}, 200},
				{"regionId=2&iata=ykf", []string{"YKF", "YVR", "YYJ"}, 200},
				{"region=missing", nil, 400},
				{"regionId=bad", nil, 400},
			} {
				t.Run(tc.query, func(t *testing.T) {
					got, calls = nil, 0
					r := httptest.NewRequest(http.MethodGet, endpoint.path, nil)
					if r.URL.RawQuery != "" && tc.query != "" {
						r.URL.RawQuery += "&"
					}
					r.URL.RawQuery += tc.query
					w := httptest.NewRecorder()
					endpoint.handler(reader).ServeHTTP(w, r)
					if w.Code != tc.status {
						t.Fatalf("status %d, want %d: %s", w.Code, tc.status, w.Body.String())
					}
					if tc.status == 200 {
						if calls != 1 || !slices.Equal(got, tc.want) {
							t.Fatalf("reader got IATAs %v (%d values), calls %d; want %v (%d values)", got, len(got), calls, tc.want, len(tc.want))
						}
					} else if calls != 0 {
						t.Fatal("invalid region reached the list reader")
					}
				})
			}
		})
	}
}
