// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

type pathReader struct {
	stubReader
	read func(context.Context, time.Time, time.Time, []string) (*api.PathStats, error)
}

func (s pathReader) GetPathStats(ctx context.Context, since, until time.Time, iatas []string) (*api.PathStats, error) {
	return s.read(ctx, since, until, iatas)
}

func TestPathsRejectInvalidWindow(t *testing.T) {
	for _, query := range []string{"", "since=0", "since=x&until=1", "since=-1&until=1", "since=1&until=1", "since=2&until=1", "since=0&until=2592000001", "since=0&since=0&until=1", "since=0&until=1&until=2", "since=253402300799998&until=253402300800000"} {
		w := httptest.NewRecorder()
		StatsRouter(nil).ServeHTTP(w, httptest.NewRequest("GET", "/paths?"+query, nil))
		if w.Code != 400 {
			t.Fatalf("query %q: %d %s", query, w.Code, w.Body.String())
		}
	}
}

func TestPathsFiltersErrorsAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		iatas       []string
		err         error
		status      int
	}{
		{"global", "", nil, nil, 200}, {"IATAs", "&iatas=yvr,yyj", []string{"YVR", "YYJ"}, nil, 200},
		{"region", "&region=west", []string{"YVR"}, nil, 200}, {"empty region", "&region=empty", []string{""}, nil, 200},
		{"missing region", "&region=missing", nil, nil, 400}, {"timeout", "", nil, context.DeadlineExceeded, 503},
		{"database error", "", nil, errors.New("private detail"), 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := pathReader{stubReader: stubReader{getRegionBySlug: func(_ context.Context, slug string) (*api.Region, error) {
				if slug == "empty" {
					return &api.Region{}, nil
				}
				if slug == "missing" {
					return nil, errors.New("not found")
				}
				return &api.Region{IATAs: []string{"YVR"}}, nil
			}}, read: func(ctx context.Context, since, until time.Time, iatas []string) (*api.PathStats, error) {
				if d, ok := ctx.Deadline(); !ok || time.Until(d) > 15*time.Second {
					t.Fatal("query has no bounded deadline")
				}
				if since.UnixMilli() != 0 || until.UnixMilli() != 2592000000 || !reflect.DeepEqual(iatas, tc.iatas) {
					t.Fatalf("unexpected arguments: %v %v %v", since, until, iatas)
				}
				return &api.PathStats{Until: until.UnixMilli(), Receptions: 4, Hashed: 1, Empty: 1, Trace: 1, Unclassified: 1}, tc.err
			}}
			w := httptest.NewRecorder()
			StatsRouter(reader).ServeHTTP(w, httptest.NewRequest("GET", "/paths?since=0&until=2592000000"+tc.query, nil))
			if w.Code != tc.status || strings.Contains(w.Body.String(), "private detail") {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			if tc.status == 200 {
				var got api.PathStats
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.Receptions != 4 || got.Trace != 1 {
					t.Fatalf("response: %+v %v", got, err)
				}
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	r := pathReader{read: func(ctx context.Context, _ time.Time, _ time.Time, _ []string) (*api.PathStats, error) {
		called = true
		return nil, ctx.Err()
	}}
	w := httptest.NewRecorder()
	StatsRouter(r).ServeHTTP(w, httptest.NewRequest("GET", "/paths?since=0&until=1", nil).WithContext(ctx))
	if !called || w.Body.Len() != 0 {
		t.Fatal("cancellation did not reach reader")
	}
}
