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

type signalReader struct {
	stubReader
	read func(context.Context, time.Time, time.Time, []string) (*api.SignalStats, error)
}

func (s signalReader) GetSignalStats(ctx context.Context, since, until time.Time, iatas []string) (*api.SignalStats, error) {
	return s.read(ctx, since, until, iatas)
}

func TestSignalRejectsInvalidWindow(t *testing.T) {
	for _, query := range []string{"", "since=0", "until=1", "since=x&until=1", "since=-1&until=1", "since=0&until=0", "since=2&until=1", "since=0&until=2592000001", "since=0&since=0&until=1", "since=0&until=1&until=2", "since=253402300799998&until=253402300800000"} {
		w := httptest.NewRecorder()
		StatsRouter(nil).ServeHTTP(w, httptest.NewRequest("GET", "/signal?"+query, nil))
		if w.Code != 400 {
			t.Fatalf("query %q: %d %s", query, w.Code, w.Body.String())
		}
	}
}

func TestSignalFiltersAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		iatas       []string
		err         error
		status      int
	}{
		{"global", "", nil, nil, 200},
		{"IATAs", "&iatas=yvr,yyj", []string{"YVR", "YYJ"}, nil, 200},
		{"region", "&region=west", []string{"YVR"}, nil, 200},
		{"empty region", "&region=empty", []string{""}, nil, 200},
		{"missing region", "&region=missing", nil, nil, 400},
		{"timeout", "", nil, context.DeadlineExceeded, 503},
		{"database error", "", nil, errors.New("private detail"), 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := signalReader{stubReader: stubReader{getRegionBySlug: func(_ context.Context, slug string) (*api.Region, error) {
				switch slug {
				case "empty":
					return &api.Region{}, nil
				case "missing":
					return nil, errors.New("not found")
				default:
					return &api.Region{IATAs: []string{"YVR"}}, nil
				}
			}}, read: func(ctx context.Context, since, until time.Time, iatas []string) (*api.SignalStats, error) {
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 15*time.Second {
					t.Fatal("missing bounded query deadline")
				}
				if since.UnixMilli() != 0 || until.UnixMilli() != 2592000000 || !reflect.DeepEqual(iatas, tc.iatas) {
					t.Fatalf("unexpected arguments: %v %v %v", since, until, iatas)
				}
				return &api.SignalStats{Until: until.UnixMilli(), Receptions: 5, Hourly: []api.SignalHour{}}, tc.err
			}}
			w := httptest.NewRecorder()
			StatsRouter(reader).ServeHTTP(w, httptest.NewRequest("GET", "/signal?since=0&until=2592000000"+tc.query, nil))
			if w.Code != tc.status || strings.Contains(w.Body.String(), "private detail") {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			if tc.status == 200 {
				var got api.SignalStats
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.Receptions != 5 || got.SNR.Average != nil {
					t.Fatalf("invalid response: %+v %v", got, err)
				}
			}
		})
	}
}

func TestSignalCancellationReachesReader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	reader := signalReader{read: func(ctx context.Context, _ time.Time, _ time.Time, _ []string) (*api.SignalStats, error) {
		called = true
		return nil, ctx.Err()
	}}
	w := httptest.NewRecorder()
	StatsRouter(reader).ServeHTTP(w, httptest.NewRequest("GET", "/signal?since=0&until=1", nil).WithContext(ctx))
	if !called || w.Body.Len() != 0 {
		t.Fatalf("cancellation: called=%v body=%s", called, w.Body.String())
	}
}

func TestStatsWindowSnapsPollingTimes(t *testing.T) {
	for _, query := range []string{"since=123&until=86400123", "since=3599999&until=89999999"} {
		since, until, err := parseStatsWindow(httptest.NewRequest("GET", "/?"+query, nil))
		if err != nil || since.UnixMilli() != 0 || until.UnixMilli() != 86400000 {
			t.Fatalf("%v %v %v", since, until, err)
		}
	}
	since, until, err := parseStatsWindow(httptest.NewRequest("GET", "/?since=1&until=2", nil))
	if err != nil || !since.Equal(until) {
		t.Fatal("sub-hour normalization")
	}
}
