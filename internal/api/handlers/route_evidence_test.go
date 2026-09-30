// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
)

const evidenceKey = "f097439148601d9f3291c474f82fa64c"

func TestRouteEvidenceQuery(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	q, err := routeEvidenceQuery(httptest.NewRequest("GET", "/", nil), "YOW", evidenceKey, now)
	if err != nil || q.Limit != 50 || q.Until != now || q.Until.Sub(q.Since) != 24*time.Hour {
		t.Fatalf("defaults: %+v %v", q, err)
	}
	week, err := routeEvidenceQuery(httptest.NewRequest("GET", "/?range=168h", nil), "YOW", evidenceKey, now)
	if err != nil || !week.Until.Equal(now) || week.Until.Sub(week.Since) != 7*24*time.Hour {
		t.Fatalf("server anchored range: %+v %v", week, err)
	}
	c := api.RouteEvidenceCursor{IATA: "YOW", PathKey: evidenceKey, Since: q.Since, Until: q.Until, HeardAt: q.Since.Add(time.Hour + 123*time.Microsecond), ID: 3}
	next, err := routeEvidenceQuery(httptest.NewRequest("GET", "/?pageCursor="+url.QueryEscape(c.String())+"&limit=999", nil), "YOW", evidenceKey, now.Add(time.Hour))
	if err != nil || next.Limit != 200 || !next.Until.Equal(now) || !next.Cursor.HeardAt.Equal(c.HeardAt) {
		t.Fatalf("pinned cursor: %+v %v", next, err)
	}
	for _, query := range []string{"?limit=0", "?limit=-1", "?limit=a", "?limit=1&limit=2", "?since=0", "?until=10", "?since=2&until=1", "?since=0&until=2678400000", "?since=0&until=99999999999999999", "?pageCursor=bad", "?pageCursor=", "?cursor=123", "?pageCursor=" + url.QueryEscape(c.String()) + "&since=0&until=10"} {
		if _, err := routeEvidenceQuery(httptest.NewRequest("GET", "/"+query, nil), "YOW", evidenceKey, now); err == nil {
			t.Errorf("accepted %s", query)
		}
	}
	if _, err := routeEvidenceQuery(httptest.NewRequest("GET", "/?pageCursor="+url.QueryEscape(c.String()), nil), "YVR", evidenceKey, now); err == nil {
		t.Fatal("cursor crossed IATA")
	}
	for _, query := range []string{"range=0h", "range=-1h", "range=721h", "range=1ns", "range=7d", "range=24h&range=168h", "range=24h&since=0&until=10", "range=24h&pageCursor=" + url.QueryEscape(c.String())} {
		if _, err := routeEvidenceQuery(httptest.NewRequest("GET", "/?"+query, nil), "YOW", evidenceKey, now); err == nil {
			t.Errorf("accepted %s", query)
		}
	}
	ahead := now.Add(time.Minute)
	if _, err := routeEvidenceQuery(httptest.NewRequest("GET", fmt.Sprintf("/?since=%d&until=%d", ahead.Add(-time.Hour).UnixMilli(), ahead.UnixMilli()), nil), "YOW", evidenceKey, now); err != nil {
		t.Fatalf("rejected until within clock skew: %v", err)
	}
	far := now.Add(10 * time.Minute)
	if _, err := routeEvidenceQuery(httptest.NewRequest("GET", fmt.Sprintf("/?since=%d&until=%d", far.Add(-time.Hour).UnixMilli(), far.UnixMilli()), nil), "YOW", evidenceKey, now); err == nil {
		t.Fatal("accepted until far in the future")
	}
}

func TestRouteEvidenceHandler(t *testing.T) {
	for _, tc := range []struct {
		path   string
		err    error
		status int
	}{
		{"/yow/" + evidenceKey + "/observations", nil, 200},
		{"/YOW/bad/observations", nil, 400},
		{"/LONG/" + evidenceKey + "/observations", nil, 400},
		{"/YOW/" + evidenceKey + "/observations", pgx.ErrNoRows, 404},
		{"/YOW/" + evidenceKey + "/observations", errors.New("database unavailable"), 500},
	} {
		called := false
		router := RoutesRouter(stubReader{getRouteEvidence: func(_ context.Context, iata, key string, q api.RouteEvidenceQuery) (*api.RouteEvidence, error) {
			called = true
			if iata != "YOW" || key != evidenceKey || q.Limit != 50 {
				t.Fatal("wrong reader scope")
			}
			return &api.RouteEvidence{Items: []api.RouteObservation{}}, tc.err
		}})
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
		if tc.status == 400 && called {
			t.Fatal("invalid request reached database")
		}
	}
}
