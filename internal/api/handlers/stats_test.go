// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

func TestGetStatsObservations_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/observations", getStatsObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/observations?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsPayloadBreakdown_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/payload-breakdown", getStatsPayloadBreakdown(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/payload-breakdown?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopNodes_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-nodes", getStatsTopNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-nodes?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopObservers_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-observers", getStatsTopObservers(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-observers?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopObservers_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-observers", getStatsTopObservers(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-observers?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsOverview_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/overview", getStatsOverview(stubReader{
		getStatsOverview: func(_ context.Context, _ []string) (*api.StatsOverview, error) {
			return &api.StatsOverview{TotalPackets: 100}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats/overview", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetStatsObservations_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/observations", getStatsObservations(stubReader{
		getStatsObservations: func(_ context.Context, _ []string, _ time.Time) ([]api.ObservationPoint, error) {
			return []api.ObservationPoint{{IATA: "YVR", ObservationCount: 10}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats/observations", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetStatsPayloadBreakdown_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/payload-breakdown", getStatsPayloadBreakdown(stubReader{
		getStatsPayloadBreakdown: func(_ context.Context, _ []string, _ time.Time) ([]api.PayloadBreakdownItem, error) {
			return []api.PayloadBreakdownItem{{PayloadType: 4, Count: 100}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats/payload-breakdown", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetStatsTopNodes_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-nodes", getStatsTopNodes(stubReader{
		getStatsTopNodes: func(_ context.Context, _ []string, _ int32) ([]api.TopNode, error) {
			return []api.TopNode{{IATA: "YVR", ObservationCount: 50}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-nodes", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetStatsTopObservers_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-observers", getStatsTopObservers(stubReader{
		getStatsTopObservers: func(_ context.Context, _ []string, _ time.Time, _ int32) ([]api.TopObserver, error) {
			return []api.TopObserver{{IATA: "YVR", ObservationCount: 20}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-observers", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetStatsTopAdvertisers_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-advertisers", getStatsTopAdvertisers(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-advertisers?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopAdvertisers_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-advertisers", getStatsTopAdvertisers(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-advertisers?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopAdvertisers_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-advertisers", getStatsTopAdvertisers(stubReader{
		getStatsTopAdvertisers: func(_ context.Context, _ []string, _ time.Time, _ int32) ([]api.TopAdvertiser, error) {
			return []api.TopAdvertiser{{IATA: "YVR", AdvertCount: 5}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-advertisers", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetStatsClockDrift_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/clock-drift", getStatsClockDrift(stubReader{
		getStatsClockDrift: func(_ context.Context, _ []string, _ int32) ([]api.ClockDriftEntry, error) {
			return []api.ClockDriftEntry{{ClockDriftSeconds: -600}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats/clock-drift", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetStatsClockDrift_MultiIATA_PassedThrough(t *testing.T) {
	var gotIATAs []string
	r := chi.NewRouter()
	r.Get("/stats/clock-drift", getStatsClockDrift(stubReader{
		getStatsClockDrift: func(_ context.Context, iatas []string, _ int32) ([]api.ClockDriftEntry, error) {
			gotIATAs = iatas
			return nil, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats/clock-drift?iatas=YVR,YYJ", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if len(gotIATAs) != 2 || gotIATAs[0] != "YVR" || gotIATAs[1] != "YYJ" {
		t.Errorf("expected [YVR YYJ], got %v", gotIATAs)
	}
}

func TestGetStatsClockDrift_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/clock-drift", getStatsClockDrift(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/clock-drift?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopTalkers_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-talkers", getStatsTopTalkers(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-talkers?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopTalkers_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-talkers", getStatsTopTalkers(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-talkers?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStatsTopTalkers_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/top-talkers", getStatsTopTalkers(stubReader{
		getStatsTopTalkers: func(_ context.Context, _ []string, _ time.Time, _ int32) ([]api.TopTalker, error) {
			return []api.TopTalker{{SenderName: "Robbie", MessageCount: 3}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats/top-talkers", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetStatsRadioPresets_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/radio-presets", getStatsRadioPresets(stubReader{
		getRadioPresets: func(_ context.Context, _ string, _ []string) ([]api.RadioPreset, error) {
			return []api.RadioPreset{{Preset: "915.0,125,7", IATA: "YVR"}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats/radio-presets", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetStatsScopes_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/scopes", getStatsScopes(stubReader{
		getScopeStats: func(_ context.Context, _ []string) ([]api.ScopeStats, error) {
			return []api.ScopeStats{{Name: "#bc", PacketCount: 100}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats/scopes", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetStatsNodeTypes_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/stats/node-types", getStatsNodeTypes(stubReader{
		getStatsNodeTypes: func(_ context.Context, _ []string) ([]api.NodeTypeCount, error) {
			return []api.NodeTypeCount{{NodeType: 1, Count: 5}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats/node-types", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}
