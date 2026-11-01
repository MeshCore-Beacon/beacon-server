// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

func TestSearchKnownRoutes_MissingParams(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/routes/search", searchKnownRoutes(stubReader{}))

	tests := []struct {
		name  string
		query string
	}{
		{"missing all", ""},
		{"missing from and to", "?iata=YVR"},
		{"missing to", "?iata=YVR&from=aa"},
		{"bad hex", "?iata=YVR&from=zz&to=bb"},
		{"too many iatas", "?from=aa&to=bb&iatas=" + manyIATAs(maxRouteSearchIATAs+1)},
		{"long code", "?from=aa&to=bb&iatas=YVRX"},
		{"huge list", "?from=aa&to=bb&iatas=" + manyIATAs(200000)},
	}
	for _, tt := range tests {
		req := httptest.NewRequest(http.MethodGet, "/routes/search"+tt.query, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d", tt.name, w.Code)
		}
	}
}

func TestSearchCrossIATARoutes_BadParams(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/routes/cross", searchCrossIATARoutes(stubReader{}))

	many := manyIATAs(maxRouteSearchIATAs + 1)
	tests := []struct {
		name  string
		query string
	}{
		{"missing all", ""},
		{"missing toHash", "?fromHash=aa&fromIata=YVR&toIata=YYJ"},
		{"missing fromIata", "?fromHash=aa&toHash=bb&toIata=YYJ"},
		{"missing fromHash", "?fromIata=YVR&toHash=bb&toIata=YYJ"},
		{"bad hex", "?fromHash=zz&toHash=bb"},
		{"odd hex", "?fromHash=aab&toHash=bb"},
		{"hash too long", "?fromHash=aabbccddee&toHash=bb"},
		{"iatas and pair", "?fromHash=aa&toHash=bb&fromIata=YVR&toIata=YYJ&iatas=YVR,YYJ"},
		{"one iata", "?fromHash=aa&toHash=bb&iatas=YVR,yvr"},
		{"too many iatas", "?fromHash=aa&toHash=bb&iatas=" + many},
	}
	for _, tt := range tests {
		req := httptest.NewRequest(http.MethodGet, "/routes/cross"+tt.query, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d", tt.name, w.Code)
		}
	}
}

func TestListKnownRoutes_CursorID(t *testing.T) {
	var gotCursor time.Time
	var gotID int64
	r := chi.NewRouter()
	r.Get("/routes", listKnownRoutes(stubReader{
		listKnownRoutes: func(_ context.Context, _ []string, _ int32, cursor time.Time, cursorID int64, _ int32) ([]api.KnownRoute, error) {
			gotCursor, gotID = cursor, cursorID
			return nil, nil
		},
	}))
	for _, tc := range []struct {
		query string
		code  int
		id    int64
	}{
		{"cursor=1700000000000&cursorId=42", http.StatusOK, 42},
		{"cursor=1700000000000", http.StatusOK, 0},
		{"cursor=1700000000000&cursorId=0", http.StatusBadRequest, 0},
		{"cursor=1700000000000&cursorId=x", http.StatusBadRequest, 0},
		{"cursorId=42", http.StatusBadRequest, 0},
	} {
		gotID = 0
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/routes?"+tc.query, nil))
		if w.Code != tc.code {
			t.Errorf("%s: code %d, want %d", tc.query, w.Code, tc.code)
			continue
		}
		if tc.code == http.StatusOK && (gotID != tc.id || gotCursor.UnixMilli() != 1700000000000) {
			t.Errorf("%s: cursor %v id %d", tc.query, gotCursor, gotID)
		}
	}
}

func TestListKnownRoutes_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/routes", listKnownRoutes(stubReader{
		listKnownRoutes: func(_ context.Context, _ []string, _ int32, _ time.Time, _ int64, _ int32) ([]api.KnownRoute, error) {
			return []api.KnownRoute{{IATA: "YVR"}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/routes", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// iatas takes precedence and is parsed like /routes/search's; a lone iata reaches the store exactly as sent.
func TestListKnownRoutes_IATAs(t *testing.T) {
	var got []string
	var gotHop, gotLimit int32
	calls := 0
	r := chi.NewRouter()
	r.Get("/routes", listKnownRoutes(stubReader{
		listKnownRoutes: func(_ context.Context, iatas []string, hop int32, _ time.Time, _ int64, limit int32) ([]api.KnownRoute, error) {
			calls++
			got, gotHop, gotLimit = iatas, hop, limit
			return []api.KnownRoute{}, nil
		},
	}))
	hundred := manyIATAs(maxRouteSearchIATAs)
	for _, tt := range []struct {
		query     string
		code      int
		want      []string
		hop, limt int32
	}{
		{"", http.StatusOK, nil, 0, 50},
		{"iata=SEA", http.StatusOK, []string{"SEA"}, 0, 50},
		{"iata=sea", http.StatusOK, []string{"sea"}, 0, 50},
		{"iata=SEAX", http.StatusOK, []string{"SEAX"}, 0, 50},
		{"iatas=sea,KLS,%20pdx%20,SEA,", http.StatusOK, []string{"SEA", "KLS", "PDX"}, 0, 50},
		{"iatas=KLS", http.StatusOK, []string{"KLS"}, 0, 50},
		{"iatas=KLS,SEA&iata=YVR", http.StatusOK, []string{"KLS", "SEA"}, 0, 50},
		{"iatas=&iata=YVR", http.StatusOK, []string{"YVR"}, 0, 50},
		{"iatas=SEA,KLS&hopCount=3&limit=500", http.StatusOK, []string{"SEA", "KLS"}, 3, 200},
		{"iatas=" + hundred, http.StatusOK, strings.Split(hundred, ","), 0, 50},
		{"iatas=" + hundred + ",AAA", http.StatusOK, strings.Split(hundred, ","), 0, 50},
		{"iatas=SEAX", http.StatusBadRequest, nil, 0, 0},
		{"iatas=SEA,SE", http.StatusBadRequest, nil, 0, 0},
		{"iatas=SEAX&iata=SEA", http.StatusBadRequest, nil, 0, 0},
		{"iatas=,", http.StatusBadRequest, nil, 0, 0},
		{"iatas=%20,%20", http.StatusBadRequest, nil, 0, 0},
		{"iatas=" + manyIATAs(maxRouteSearchIATAs+1), http.StatusBadRequest, nil, 0, 0},
		{"iatas=" + manyIATAs(200000), http.StatusBadRequest, nil, 0, 0},
		{"iatas=SEA,KLS&limit=0", http.StatusBadRequest, nil, 0, 0},
	} {
		calls, got, gotHop, gotLimit = 0, nil, 0, 0
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/routes?"+tt.query, nil))
		name := tt.query
		if len(name) > 60 {
			name = name[:60] + "..."
		}
		if w.Code != tt.code {
			t.Errorf("%s: code %d, want %d (%s)", name, w.Code, tt.code, w.Body.String())
			continue
		}
		if tt.code != http.StatusOK {
			if calls != 0 {
				t.Errorf("%s: the store ran on a 400", name)
			}
			continue
		}
		if calls != 1 || !slices.Equal(got, tt.want) || gotHop != tt.hop || gotLimit != tt.limt {
			t.Errorf("%s: calls %d, iatas %v hop %d limit %d; want %v hop %d limit %d", name, calls, got, gotHop, gotLimit, tt.want, tt.hop, tt.limt)
		}
	}
}

func TestSearchKnownRoutes_OK(t *testing.T) {
	var got []string
	r := chi.NewRouter()
	r.Get("/routes/search", searchKnownRoutes(stubReader{
		searchKnownRoutes: func(_ context.Context, iatas []string, _, _ string) ([]api.KnownRoute, error) {
			got = iatas
			return []api.KnownRoute{{IATA: "YVR"}}, nil
		},
	}))
	for _, tt := range []struct {
		query string
		want  []string
	}{
		{"?iata=yvr&from=aa&to=bb", []string{"YVR"}},
		{"?iatas=yvr,YYJ,YVR&from=aa&to=bbcc", []string{"YVR", "YYJ"}},
		{"?from=aa&to=bb", nil},
	} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/routes/search"+tt.query, nil))
		if w.Code != http.StatusOK || !slices.Equal(got, tt.want) {
			t.Errorf("%s: got %d, iatas %v", tt.query, w.Code, got)
		}
	}
}

func TestSearchCrossIATARoutes_Query(t *testing.T) {
	var got api.CrossRouteSearch
	r := chi.NewRouter()
	r.Get("/routes/cross", searchCrossIATARoutes(stubReader{
		searchCrossIATARoutes: func(_ context.Context, q api.CrossRouteSearch) ([]api.CrossIATARoute, error) {
			got = q
			return nil, nil
		},
	}))
	for _, tt := range []struct {
		query    string
		from, to []string
	}{
		{"?fromHash=AA&fromIata=yvr&toHash=bb&toIata=YYJ", []string{"YVR"}, []string{"YYJ"}},
		{"?fromHash=aa&toHash=bb&iatas=yvr,YYJ,YVR", []string{"YVR", "YYJ"}, []string{"YVR", "YYJ"}},
		{"?fromHash=aa&toHash=bbcc", nil, nil},
	} {
		req := httptest.NewRequest(http.MethodGet, "/routes/cross"+tt.query, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != "[]" {
			t.Errorf("%s: got %d %q", tt.query, w.Code, w.Body.String())
		}
		if got.FromHash != "aa" || !slices.Equal(got.FromIATAs, tt.from) || !slices.Equal(got.ToIATAs, tt.to) {
			t.Errorf("%s: reader got %+v", tt.query, got)
		}
	}
}

func TestSearchCrossIATARoutes_TooBroad(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/routes/cross", searchCrossIATARoutes(stubReader{
		searchCrossIATARoutes: func(context.Context, api.CrossRouteSearch) ([]api.CrossIATARoute, error) {
			return nil, api.ErrRouteSearchTooBroad
		},
	}))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/routes/cross?fromHash=aa&toHash=bb", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func manyIATAs(n int) string {
	codes := make([]string, n)
	for i := range codes {
		codes[i] = fmt.Sprintf("%c%c%c", 'A'+i/676%26, 'A'+i/26%26, 'A'+i%26)
	}
	return strings.Join(codes, ",")
}

func TestRouteSearch_ClientGoneWritesNothing(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/routes/search", searchKnownRoutes(stubReader{
		searchKnownRoutes: func(context.Context, []string, string, string) ([]api.KnownRoute, error) {
			return nil, context.Canceled
		},
	}))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/routes/search?from=aa&to=bb", nil))
	if w.Body.Len() != 0 {
		t.Errorf("expected no body, got %d %q", w.Code, w.Body.String())
	}
}
