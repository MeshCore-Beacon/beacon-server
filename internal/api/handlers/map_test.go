package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MeshCore-Tower/tower-server/internal/api"
)

func TestMapStateFiltersByIATA(t *testing.T) {
	reader := &fakeMapReader{}
	req := httptest.NewRequest(http.MethodGet, "/state?iata=yow", nil)
	rec := httptest.NewRecorder()

	MapRouter(reader).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := reader.filter.IATAs; len(got) != 1 || got[0] != "YOW" {
		t.Fatalf("IATAs = %#v, want [YOW]", got)
	}

	var state api.MapState
	if err := json.NewDecoder(rec.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	if state.Metadata.LiveDefaultEnabled {
		t.Fatal("live map layer must default off")
	}
}

func TestMapStateRejectsMixedScopes(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/state?iata=YOW&regionId=1", nil)
	rec := httptest.NewRecorder()

	MapRouter(&fakeMapReader{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

type fakeMapReader struct {
	filter api.MapStateFilter
}

func (f *fakeMapReader) ListIATAs(context.Context) ([]api.IATA, error) {
	return nil, nil
}

func (f *fakeMapReader) GetIATA(context.Context, string) (*api.IATA, error) {
	return nil, nil
}

func (f *fakeMapReader) ListRegions(context.Context) ([]api.RegionSummary, error) {
	return nil, nil
}

func (f *fakeMapReader) GetRegion(context.Context, int32) (*api.Region, error) {
	return &api.Region{IATAs: []string{"YOW", "YUL"}}, nil
}

func (f *fakeMapReader) GetMapState(_ context.Context, filter api.MapStateFilter) (*api.MapState, error) {
	f.filter = filter
	return &api.MapState{
		ServerTime: 1,
		Scope:      api.MapScope{IATAs: filter.IATAs, RegionID: filter.RegionID},
		Metadata: api.MapMetadata{
			Basemap:            "openfreemap",
			RoutesComplete:     false,
			RoutesStatus:       "blocked_by_ordered_path_confidence",
			LiveDefaultEnabled: false,
		},
		Nodes:     []api.MapNode{},
		Observers: []api.MapObserver{},
		Routes:    []api.MapRoute{},
	}, nil
}
