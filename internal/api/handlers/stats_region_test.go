// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

// iataRecorder records the IATA filter each stats read receives.
type iataRecorder struct {
	stubReader
	got *[]string
}

func (r iataRecorder) rec(iatas []string) { *r.got = append([]string{}, iatas...) }

func (r iataRecorder) GetStatsOverview(_ context.Context, iatas []string) (*api.StatsOverview, error) {
	r.rec(iatas)
	return &api.StatsOverview{}, nil
}
func (r iataRecorder) GetStatsObservations(_ context.Context, iatas []string, _ time.Time) ([]api.ObservationPoint, error) {
	r.rec(iatas)
	return nil, nil
}
func (r iataRecorder) GetStatsPayloadBreakdown(_ context.Context, iatas []string, _ time.Time) ([]api.PayloadBreakdownItem, error) {
	r.rec(iatas)
	return nil, nil
}
func (r iataRecorder) GetStatsTopNodes(_ context.Context, iatas []string, _ time.Time, _ int32) ([]api.TopNode, error) {
	r.rec(iatas)
	return nil, nil
}
func (r iataRecorder) GetStatsTopObservers(_ context.Context, iatas []string, _ time.Time, _ int32) ([]api.TopObserver, error) {
	r.rec(iatas)
	return nil, nil
}
func (r iataRecorder) GetStatsTopAdvertisers(_ context.Context, iatas []string, _ time.Time, _ int32) ([]api.TopAdvertiser, error) {
	r.rec(iatas)
	return nil, nil
}
func (r iataRecorder) GetStatsClockDrift(_ context.Context, iatas []string, _ int32) ([]api.ClockDriftEntry, error) {
	r.rec(iatas)
	return nil, nil
}
func (r iataRecorder) GetStatsTopTalkers(_ context.Context, iatas []string, _ time.Time, _ int32) ([]api.TopTalker, error) {
	r.rec(iatas)
	return nil, nil
}
func (r iataRecorder) GetRadioPresets(_ context.Context, _ string, iatas []string) ([]api.RadioPreset, error) {
	r.rec(iatas)
	return nil, nil
}
func (r iataRecorder) GetStatsNodeTypes(_ context.Context, iatas []string) ([]api.NodeTypeCount, error) {
	r.rec(iatas)
	return nil, nil
}

// A region with no IATAs must filter to nothing, not fall back to every IATA.
func TestStatsEmptyRegionMatchesNothing(t *testing.T) {
	for _, path := range []string{"overview", "observations", "payload-breakdown", "top-nodes", "top-observers",
		"top-advertisers", "clock-drift", "top-talkers", "radio-presets", "node-types"} {
		for _, tc := range []struct {
			region string
			want   []string
		}{
			{"empty", []string{""}},
			{"west", []string{"YVR"}},
		} {
			var got []string
			reader := iataRecorder{got: &got, stubReader: stubReader{getRegionBySlug: func(_ context.Context, slug string) (*api.Region, error) {
				if slug == "empty" {
					return &api.Region{}, nil
				}
				return &api.Region{IATAs: []string{"YVR"}}, nil
			}}}
			w := httptest.NewRecorder()
			StatsRouter(reader, StatsOptions{}).ServeHTTP(w, httptest.NewRequest("GET", "/"+path+"?region="+tc.region, nil))
			if w.Code != 200 || !reflect.DeepEqual(got, tc.want) {
				t.Errorf("/stats/%s?region=%s: status %d, iatas %q; want 200, %q", path, tc.region, w.Code, got, tc.want)
			}
		}
	}
}
