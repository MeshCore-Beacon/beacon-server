// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

func TestDirectoryRejectsInvalidPagination(t *testing.T) {
	for _, q := range []string{"sort=bogus", "cursor=1", "cursor=-1", "limit=0", "snapshot=bad", "until=no", "since=0", "unknown=1", "snapshot=00000000-0000-0000-0000-000000000001&sort=name", "status=maybe", "sort=name&sort=traffic", "cursor=2147483448", "since=1767229200000&until=1767225600000", "since=1764543600000&until=1767229200000", "until=9223372036854775807"} {
		t.Run(q, func(t *testing.T) {
			r := stubReader{listObserverDirectory: func(context.Context, api.ObserverDirectoryQuery) (*api.ObserverDirectory, error) {
				t.Fatal("invalid query reached store")
				return nil, nil
			}}
			w := httptest.NewRecorder()
			ObserversRouter(r).ServeHTTP(w, httptest.NewRequest("GET", "/directory?"+q, nil))
			if w.Code != 400 {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestDirectoryContinuationAndErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code int
	}{{nil, 200}, {errors.New("database unavailable"), 500}} {
		r := stubReader{listObserverDirectory: func(_ context.Context, q api.ObserverDirectoryQuery) (*api.ObserverDirectory, error) {
			if q.Sort != "name" || q.Name != "alpha" || q.Since != 1767225600123 || q.Until != 1767229200789 || q.Cursor != 50 || q.Limit != 25 {
				t.Fatalf("query %+v", q)
			}
			return &api.ObserverDirectory{}, tc.err
		}}
		w := httptest.NewRecorder()
		ObserversRouter(r).ServeHTTP(w, httptest.NewRequest("GET", "/directory?sort=name&name=alpha&since=1767225600123&until=1767229200789&cursor=50&limit=25", nil))
		if w.Code != tc.code {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
	}
}

func TestDirectoryNormalizesFirstPage(t *testing.T) {
	r := stubReader{listObserverDirectory: func(_ context.Context, q api.ObserverDirectoryQuery) (*api.ObserverDirectory, error) {
		if q.Sort != "traffic" || q.Limit != 200 || q.Name != "alpha" || q.Since != 1767225600999 || q.Until != 1767229200999 || len(q.IATAs) != 2 || q.IATAs[0] != "YVR" || q.IATAs[1] != "YYJ" {
			t.Fatalf("query %+v", q)
		}
		return &api.ObserverDirectory{}, nil
	}}
	w := httptest.NewRecorder()
	ObserversRouter(r).ServeHTTP(w, httptest.NewRequest("GET", "/directory?iatas=yyj,yvr,YVR&name=ALPHA&since=1767225600999&until=1767229200999&limit=201", nil))
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("response %d %s", w.Code, w.Body.String())
	}
}

func TestDirectoryRollingWeekAndContinuation(t *testing.T) {
	var queries []api.ObserverDirectoryQuery
	r := stubReader{listObserverDirectory: func(_ context.Context, q api.ObserverDirectoryQuery) (*api.ObserverDirectory, error) {
		queries = append(queries, q)
		return &api.ObserverDirectory{WindowStart: q.Since, WindowEnd: q.Until}, nil
	}}
	router := ObserversRouter(r)
	before := time.Now().UnixMilli()
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/directory?iatas=YVR,YYJ&limit=1", nil))
	after := time.Now().UnixMilli()
	if w.Code != 200 || len(queries) != 1 {
		t.Fatalf("response %d: %s", w.Code, w.Body.String())
	}
	q := queries[0]
	if q.Until < before || q.Until > after || q.Until-q.Since != (7*24*time.Hour).Milliseconds() {
		t.Fatalf("window [%d,%d) does not span seven days through the request time", q.Since, q.Until)
	}
	var page api.ObserverDirectory
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", fmt.Sprintf("/directory?iatas=YVR,YYJ&limit=1&cursor=1&since=%d&until=%d", page.WindowStart, page.WindowEnd), nil))
	if w.Code != 200 || len(queries) != 2 {
		t.Fatalf("continuation %d: %s", w.Code, w.Body.String())
	}
	if next := queries[1]; next.Since != q.Since || next.Until != q.Until || next.Sort != "traffic" || !slices.Equal(next.IATAs, q.IATAs) {
		t.Fatalf("continuation changed the window or filters: %+v", next)
	}
}

func TestDirectoryExactWindowValidation(t *testing.T) {
	const until int64 = 1767229200123
	for _, tc := range []struct {
		name, query string
		code        int
		since       int64
	}{
		{"until only defaults to seven days", fmt.Sprintf("until=%d", until), 200, until - 604800000},
		{"millisecond window", fmt.Sprintf("since=%d&until=%d", until-1, until), 200, until - 1},
		{"31 days", fmt.Sprintf("since=%d&until=%d", until-2678400000, until), 200, until - 2678400000},
		{"over 31 days", fmt.Sprintf("since=%d&until=%d", until-2678400001, until), 400, 0},
		{"empty window", fmt.Sprintf("since=%d&until=%d", until, until), 400, 0},
		{"repeated bound", fmt.Sprintf("since=1&since=2&until=%d", until), 400, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := stubReader{listObserverDirectory: func(_ context.Context, q api.ObserverDirectoryQuery) (*api.ObserverDirectory, error) {
				if tc.code != 200 || q.Since != tc.since || q.Until != until {
					t.Fatalf("unexpected query: %+v", q)
				}
				return &api.ObserverDirectory{}, nil
			}}
			w := httptest.NewRecorder()
			ObserversRouter(r).ServeHTTP(w, httptest.NewRequest("GET", "/directory?"+tc.query, nil))
			if w.Code != tc.code {
				t.Fatalf("response %d: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestDirectoryRegionFilters(t *testing.T) {
	for _, tc := range []struct {
		query string
		iatas []string
		none  bool
	}{
		{"region=west", []string{"YVR", "YYJ"}, false},
		{"regionId=7&iata=YOW", []string{"YOW", "YVR", "YYJ"}, false},
		{"region=empty", nil, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			r := stubReader{
				getRegion: func(context.Context, int32) (*api.Region, error) {
					return &api.Region{IATAs: []string{"YYJ", "YVR"}}, nil
				},
				getRegionBySlug: func(_ context.Context, slug string) (*api.Region, error) {
					if slug == "empty" {
						return &api.Region{}, nil
					}
					return &api.Region{IATAs: []string{"YYJ", "YVR"}}, nil
				},
				listObserverDirectory: func(_ context.Context, q api.ObserverDirectoryQuery) (*api.ObserverDirectory, error) {
					if !slices.Equal(q.IATAs, tc.iatas) || q.MatchNone != tc.none {
						t.Fatalf("query %+v", q)
					}
					return &api.ObserverDirectory{}, nil
				},
			}
			w := httptest.NewRecorder()
			ObserversRouter(r).ServeHTTP(w, httptest.NewRequest("GET", "/directory?"+tc.query, nil))
			if w.Code != 200 {
				t.Fatalf("response %d: %s", w.Code, w.Body.String())
			}
		})
	}
}
