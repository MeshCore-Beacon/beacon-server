// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

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
			if q.Sort != "name" || q.Name != "alpha" || q.Since != 1767225600000 || q.Until != 1767229200000 || q.Cursor != 50 || q.Limit != 25 {
				t.Fatalf("query %+v", q)
			}
			return &api.ObserverDirectory{}, tc.err
		}}
		w := httptest.NewRecorder()
		ObserversRouter(r).ServeHTTP(w, httptest.NewRequest("GET", "/directory?sort=name&name=alpha&since=1767225600000&until=1767229200000&cursor=50&limit=25", nil))
		if w.Code != tc.code {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
	}
}

func TestDirectoryNormalizesFirstPage(t *testing.T) {
	r := stubReader{listObserverDirectory: func(_ context.Context, q api.ObserverDirectoryQuery) (*api.ObserverDirectory, error) {
		if q.Sort != "traffic" || q.Limit != 200 || q.Name != "alpha" || q.Since != 1767225600000 || q.Until != 1767229200000 || len(q.IATAs) != 2 || q.IATAs[0] != "YVR" || q.IATAs[1] != "YYJ" {
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
