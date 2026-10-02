// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

func TestListScopes_NoIATAs_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/scopes", listScopes(stubReader{
		getScopeNames: func(_ context.Context) ([]string, error) {
			return []string{"#bc", "#west"}, nil
		},
	}, nil))
	req := httptest.NewRequest(http.MethodGet, "/scopes", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetScope_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/scopes/{name}", getScope(stubReader{
		getScopeByName: func(_ context.Context, name string) (*api.ScopeDetail, error) {
			return &api.ScopeDetail{Name: name}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/scopes/%23bc", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetScope_Errors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		scope *api.ScopeDetail
		err   error
		want  int
	}{
		{"no rows", nil, pgx.ErrNoRows, http.StatusNotFound},
		{"nil scope", nil, nil, http.StatusNotFound},
		{"database failure", nil, context.DeadlineExceeded, http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := chi.NewRouter()
			r.Get("/scopes/{name}", getScope(stubReader{
				getScopeByName: func(context.Context, string) (*api.ScopeDetail, error) { return tc.scope, tc.err },
			}))
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/scopes/%23bc", nil))
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d", w.Code, tc.want)
			}
			var body map[string]APIError
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["error"].Message == "" {
				t.Fatalf("not a JSON error: %s", w.Body.String())
			}
		})
	}
}

func TestListScopes_FiltersByMembership(t *testing.T) {
	reader := stubReader{
		getScopeNames: func(context.Context) ([]string, error) {
			t.Fatal("filtered list must not fall back to every stored name")
			return nil, nil
		},
		getRegionBySlug: func(_ context.Context, slug string) (*api.Region, error) {
			if slug == "empty" {
				return &api.Region{}, nil
			}
			return &api.Region{IATAs: []string{"YOW", "YGK"}}, nil
		},
	}
	members := testScopes(map[string][]string{"YOW": {"#on", "#yow"}, "YGK": {"#on"}, "YUL": {"#qc"}})
	for _, tc := range []struct {
		query   string
		members ScopeMembership
		want    []string
	}{
		{"iatas=yow", members, []string{"#on", "#yow"}},
		{"iatas=YUL,YYZ", members, []string{"#qc"}},
		{"region=ottawa", members, []string{"#on", "#yow"}},
		{"region=empty", members, []string{}},
		{"iatas=YOW", nil, []string{}},
	} {
		w := httptest.NewRecorder()
		listScopes(reader, tc.members)(w, httptest.NewRequest(http.MethodGet, "/scopes?"+tc.query, nil))
		var got []string
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != http.StatusOK || got == nil {
			t.Fatalf("%q: status %d body %s", tc.query, w.Code, w.Body.String())
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("%q: got %v, want %v", tc.query, got, tc.want)
		}
	}
}
