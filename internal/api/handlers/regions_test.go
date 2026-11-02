// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

func TestParseIATAs_Single(t *testing.T) {
	r := &http.Request{URL: &url.URL{RawQuery: "iata=yvr"}}
	result := parseIATAs(r)
	if len(result) != 1 || result[0] != "YVR" {
		t.Errorf("expected [YVR], got %v", result)
	}
}

func TestParseIATAs_Multiple(t *testing.T) {
	r := &http.Request{URL: &url.URL{RawQuery: "iatas=yvr,yyj,yyc"}}
	result := parseIATAs(r)
	if len(result) != 3 {
		t.Fatalf("expected 3 IATAs, got %d", len(result))
	}
	if result[0] != "YVR" || result[1] != "YYJ" || result[2] != "YYC" {
		t.Errorf("unexpected IATAs: %v", result)
	}
}

func TestParseIATAs_MultiplePreferredOverSingle(t *testing.T) {
	// iatas param takes precedence over iata
	r := &http.Request{URL: &url.URL{RawQuery: "iatas=yvr,yyj&iata=yyc"}}
	result := parseIATAs(r)
	if len(result) != 2 {
		t.Fatalf("expected 2 IATAs, got %d", len(result))
	}
}

func TestParseIATAs_Whitespace(t *testing.T) {
	r := &http.Request{URL: &url.URL{RawQuery: "iatas=yvr%2C+yyj"}}
	result := parseIATAs(r)
	if len(result) != 2 {
		t.Fatalf("expected 2 IATAs, got %d", len(result))
	}
	if result[1] != "YYJ" {
		t.Errorf("expected YYJ after trimming whitespace, got %s", result[1])
	}
}

func TestParseIATAs_Empty(t *testing.T) {
	r := &http.Request{URL: &url.URL{RawQuery: ""}}
	result := parseIATAs(r)
	if result != nil {
		t.Errorf("expected nil for empty query, got %v", result)
	}
}

func TestParseIATAs_Uppercase(t *testing.T) {
	r := &http.Request{URL: &url.URL{RawQuery: "iata=YVR"}}
	result := parseIATAs(r)
	if len(result) != 1 || result[0] != "YVR" {
		t.Errorf("expected [YVR], got %v", result)
	}
}

func TestListRegions_OK(t *testing.T) {
	lat, zoom := 49.2, 7
	r := chi.NewRouter()
	r.Get("/regions", listRegions(stubReader{
		listRegions: func(_ context.Context) ([]api.Region, error) {
			return []api.Region{
				{RegionSummary: api.RegionSummary{ID: 1, Slug: "bc", Name: "British Columbia"}, CenterLat: &lat, ZoomLevel: &zoom, IATAs: []string{"YVR", "YYJ"}},
				{RegionSummary: api.RegionSummary{ID: 2, Slug: "empty", Name: "Empty"}, IATAs: []string{}},
			}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/regions", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	want := `[{"id":1,"slug":"bc","name":"British Columbia","centerLat":49.2,"zoomLevel":7,"iatas":["YVR","YYJ"]},` +
		`{"id":2,"slug":"empty","name":"Empty","iatas":[]}]`
	if got := strings.TrimSpace(w.Body.String()); got != want {
		t.Errorf("body = %s\nwant %s", got, want)
	}
}

func TestGetRegion_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/regions/{regionId}", getRegion(stubReader{
		getRegion: func(_ context.Context, id int32) (*api.Region, error) {
			return &api.Region{RegionSummary: api.RegionSummary{ID: int(id), Slug: "bc"}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/regions/1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetRegion_InvalidID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/regions/{regionId}", getRegion(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/regions/notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
