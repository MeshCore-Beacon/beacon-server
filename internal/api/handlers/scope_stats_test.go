// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
	"github.com/jackc/pgx/v5"
)

func TestGetStatsScopes_RegionErrors(t *testing.T) {
	for _, query := range []string{"regionId=invalid", "regionId=2147483648", "regionId=999", "region=missing"} {
		t.Run(query, func(t *testing.T) {
			reader := stubReader{
				getRegion: func(context.Context, int32) (*api.Region, error) {
					return nil, pgx.ErrNoRows
				},
				getRegionBySlug: func(context.Context, string) (*api.Region, error) {
					return nil, pgx.ErrNoRows
				},
			}
			w := httptest.NewRecorder()
			getStatsScopes(reader, nil)(w, httptest.NewRequest(http.MethodGet, "/stats/scopes?"+query, nil))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("got status %d, want 400", w.Code)
			}
		})
	}
}

func TestGetStatsScopes_Filters(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  []string
		empty bool
	}{
		{"", nil, false},
		{"iata=yvr", []string{"YVR"}, false},
		{"iatas=yyj,%20yvr,yyj", []string{"YYJ", "YVR", "YYJ"}, false},
		{"iatas=YVR&iata=YYZ", []string{"YVR"}, false},
		{"regionId=7", []string{"YVR", "YYJ"}, false},
		{"region=west", []string{"YVR", "YYJ"}, false},
		{"regionId=7&region=missing", []string{"YVR", "YYJ"}, false},
		{"iatas=YYZ,YVR&region=west", []string{"YYZ", "YVR", "YVR", "YYJ"}, false},
		{"region=empty", nil, true},
		{"region=empty&iata=YVR", []string{"YVR"}, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			called := false
			reader := stubReader{
				getRegion: func(_ context.Context, id int32) (*api.Region, error) {
					if id != 7 {
						t.Fatalf("unexpected region ID %d", id)
					}
					return &api.Region{IATAs: []string{"YVR", "YYJ"}}, nil
				},
				getRegionBySlug: func(_ context.Context, slug string) (*api.Region, error) {
					if slug == "empty" {
						return &api.Region{}, nil
					}
					if slug != "west" {
						t.Fatalf("unexpected region slug %s", slug)
					}
					return &api.Region{IATAs: []string{"YVR", "YYJ"}}, nil
				},
				getScopeStats: func(_ context.Context, iatas []string, _ time.Time) ([]api.ScopeStats, error) {
					called = true
					if !slices.Equal(iatas, tc.want) {
						t.Fatalf("IATAs = %v, want %v", iatas, tc.want)
					}
					return []api.ScopeStats{{Name: "#test", PacketCount: 2, ObserverCount: 1, NodeCount: 3}}, nil
				},
			}
			w := httptest.NewRecorder()
			getStatsScopes(reader, testScopes(map[string][]string{"YVR": {"#test"}, "YYJ": {"#test"}}))(w, httptest.NewRequest(http.MethodGet, "/stats/scopes?"+tc.query, nil))
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", w.Code, w.Body.String())
			}
			if called == tc.empty {
				t.Fatalf("reader called = %v, empty region = %v", called, tc.empty)
			}
			var rows []api.ScopeStats
			if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil {
				t.Fatal(err)
			}
			if tc.empty {
				if rows == nil || len(rows) != 0 {
					t.Fatalf("empty region returned %s", w.Body.String())
				}
			} else if len(rows) != 1 || rows[0].PacketCount != 2 || rows[0].ObserverCount != 1 || rows[0].NodeCount != 3 {
				t.Fatalf("response changed: %+v", rows)
			}
		})
	}
}

func TestGetStatsScopes_ReaderError(t *testing.T) {
	w := httptest.NewRecorder()
	getStatsScopes(stubReader{getScopeStats: func(context.Context, []string, time.Time) ([]api.ScopeStats, error) {
		return nil, errors.New("database unavailable")
	}}, nil)(w, httptest.NewRequest(http.MethodGet, "/stats/scopes?iata=YVR", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func testScopes(byIATA map[string][]string) *scopestore.ScopeStore {
	s := scopestore.New()
	s.SetManualMembers(byIATA)
	return s
}

func TestGetStatsScopes_FiltersToRegionMembers(t *testing.T) {
	reader := stubReader{getScopeStats: func(context.Context, []string, time.Time) ([]api.ScopeStats, error) {
		return []api.ScopeStats{{Name: "#elsewhere", PacketCount: 9}, {Name: "#on"}, {Name: "#yow", PacketCount: 4}}, nil
	}}
	members := testScopes(map[string][]string{"YOW": {"#yow", "#on"}})
	for _, tc := range []struct {
		query   string
		members ScopeMembership
		want    []string
	}{
		{"", members, []string{"#elsewhere", "#on", "#yow"}},
		{"iata=YOW", members, []string{"#on", "#yow"}},
		{"iata=YUL", members, []string{}},
		{"iata=YOW", nil, []string{}},
	} {
		w := httptest.NewRecorder()
		getStatsScopes(reader, tc.members)(w, httptest.NewRequest(http.MethodGet, "/stats/scopes?"+tc.query, nil))
		var rows []api.ScopeStats
		if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil || rows == nil {
			t.Fatalf("%q: body %s, err %v", tc.query, w.Body.String(), err)
		}
		got := []string{}
		for _, row := range rows {
			got = append(got, row.Name)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("%q: got %v, want %v", tc.query, got, tc.want)
		}
	}
}
