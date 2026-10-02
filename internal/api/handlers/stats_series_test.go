// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
)

func TestGetStatsSeries(t *testing.T) {
	now := time.Now().UTC()
	until := now.Truncate(time.Hour)
	since := until.Add(-48 * time.Hour)
	window := fmt.Sprintf("since=%d&until=%d", since.UnixMilli()+1234, until.UnixMilli()+1234)
	for _, tc := range []struct {
		name, query string
		maxWindow   time.Duration
		wantSince   time.Time
		iatas       []string
		err         error
		status      int
	}{
		{"global", window, 0, since, nil, nil, 200},
		{"IATAs", window + "&iatas=yvr,yyj", 0, since, []string{"YVR", "YYJ"}, nil, 200},
		{"region", window + "&region=west", 0, since, []string{"YVR"}, nil, 200},
		{"empty region matches nothing", window + "&region=empty", 0, since, []string{""}, nil, 200},
		{"missing region", window + "&region=missing", 0, since, nil, nil, 400},
		{"missing until", fmt.Sprintf("since=%d", since.UnixMilli()), 0, since, nil, nil, 400},
		{"window over retention", window, 24 * time.Hour, since, nil, nil, 400},
		// The window fits, but its start predates what the rollups still hold.
		{"since clamped to retention", fmt.Sprintf("since=%d&until=%d", until.Add(-50*time.Hour).UnixMilli(), until.Add(-2*time.Hour).UnixMilli()),
			48 * time.Hour, until.Add(-48 * time.Hour), nil, nil, 200},
		{"timeout", window, 0, since, nil, context.DeadlineExceeded, 503},
		{"database error", window, 0, since, nil, errors.New("private detail"), 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := stubReader{
				getRegionBySlug: func(_ context.Context, slug string) (*api.Region, error) {
					switch slug {
					case "empty":
						return &api.Region{}, nil
					case "missing":
						return nil, pgx.ErrNoRows
					}
					return &api.Region{IATAs: []string{"YVR"}}, nil
				},
				getStatsSeries: func(ctx context.Context, gotSince, gotUntil time.Time, iatas []string) (*api.StatsSeries, error) {
					if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 15*time.Second {
						t.Error("missing bounded query deadline")
					}
					if !gotSince.Equal(tc.wantSince) || !reflect.DeepEqual(iatas, tc.iatas) {
						t.Errorf("args %s %v; want %s %v", gotSince, iatas, tc.wantSince, tc.iatas)
					}
					if gotUntil.Sub(gotSince) > 48*time.Hour || gotUntil.Minute() != 0 {
						t.Errorf("until %s not snapped within the window", gotUntil)
					}
					return &api.StatsSeries{Since: gotSince.UnixMilli()}, tc.err
				},
			}
			w := httptest.NewRecorder()
			StatsRouter(reader, StatsOptions{SeriesWindow: tc.maxWindow}).ServeHTTP(w, httptest.NewRequest("GET", "/series?"+tc.query, nil))
			if w.Code != tc.status || strings.Contains(w.Body.String(), "private detail") {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
		})
	}
}
