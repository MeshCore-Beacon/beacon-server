// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/cache"
)

// countingStats answers every rollup-backed stats read and counts the reads that reach it.
type countingStats struct {
	api.Reader
	calls atomic.Int64
}

func (s *countingStats) AnalyticsRevision(context.Context) (int64, error) { return 7, nil }
func (s *countingStats) GetStatsSeries(context.Context, time.Time, time.Time, []string) (*api.StatsSeries, error) {
	s.calls.Add(1)
	return &api.StatsSeries{}, nil
}
func (s *countingStats) GetSignalStats(context.Context, time.Time, time.Time, []string) (*api.SignalStats, error) {
	s.calls.Add(1)
	return &api.SignalStats{}, nil
}
func (s *countingStats) GetPathStats(context.Context, time.Time, time.Time, []string) (*api.PathStats, error) {
	s.calls.Add(1)
	return &api.PathStats{}, nil
}
func (s *countingStats) GetStatsObservations(context.Context, []string, time.Time) ([]api.ObservationPoint, error) {
	s.calls.Add(1)
	return []api.ObservationPoint{}, nil
}
func (s *countingStats) GetStatsPayloadBreakdown(context.Context, []string, time.Time) ([]api.PayloadBreakdownItem, error) {
	s.calls.Add(1)
	return []api.PayloadBreakdownItem{}, nil
}
func (s *countingStats) GetStatsTopNodes(context.Context, []string, time.Time, int32) ([]api.TopNode, error) {
	s.calls.Add(1)
	return []api.TopNode{}, nil
}
func (s *countingStats) GetStatsTopObservers(context.Context, []string, time.Time, int32) ([]api.TopObserver, error) {
	s.calls.Add(1)
	return []api.TopObserver{}, nil
}
func (s *countingStats) GetStatsTopAdvertisers(context.Context, []string, time.Time, int32, api.AdvertiserSort) ([]api.TopAdvertiser, error) {
	s.calls.Add(1)
	return []api.TopAdvertiser{}, nil
}
func (s *countingStats) GetStatsTopTalkers(context.Context, []string, time.Time, int32) ([]api.TopTalker, error) {
	s.calls.Add(1)
	return []api.TopTalker{}, nil
}
func (s *countingStats) GetScopeStats(context.Context, []string, time.Time) ([]api.ScopeStats, error) {
	s.calls.Add(1)
	return []api.ScopeStats{}, nil
}

// webDefaultViews builds the requests beacon-web makes for the default Analytics views (all
// regions, 7-day range) at now, the way its stats hooks compute their windows.
func webDefaultViews(now time.Time) []string {
	ms := now.UnixMilli()
	const hour, week = int64(3_600_000), int64(7 * 24 * 3_600_000)
	rolled := (ms-95*60_000)/hour*hour + hour // rolledWindow
	minute := ms / 60_000 * 60_000            // useWindowedStats' minuteFloor
	since := strconv.FormatInt(ms-week, 10)   // sinceFor
	f := func(v int64) string { return strconv.FormatInt(v, 10) }
	return []string{
		"/series?since=" + f(rolled-24*hour) + "&until=" + f(rolled),
		"/series?since=" + f(rolled-week) + "&until=" + f(rolled),
		"/scopes?since=" + f(rolled-week),
		"/payload-breakdown?since=" + since,
		"/top-nodes?since=" + since + "&limit=10",
		"/top-observers?since=" + since + "&limit=8",
		"/observations?since=" + since,
		"/top-advertisers?since=" + since + "&sort=flood&limit=20",
		"/top-talkers?since=" + since + "&limit=20",
		"/signal?since=" + f(minute-week) + "&until=" + f(minute),
		"/paths?since=" + f(minute-week) + "&until=" + f(minute),
	}
}

func TestStatsWarmFillsTheKeysTheWebAsksFor(t *testing.T) {
	mr := miniredis.RunT(t)
	inner := &countingStats{}
	cr := cache.NewCachedReader(inner, cache.NewClient(mr.Addr(), "", 0), cache.CacheTTLs{Stats: time.Hour}).(*cache.CachedReader)
	stats := handlers.StatsRouter(cr, handlers.StatsOptions{SeriesWindow: 90 * 24 * time.Hour})
	warmAt := time.Date(2026, 10, 27, 12, 10, 0, 0, time.UTC)
	w := NewStatsWarm(stats, cr.StatsRevision)
	w.now = func() time.Time { return warmAt }

	if err := w.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	warmed := inner.calls.Load()
	if warmed != int64(len(webDefaultViews(warmAt))) {
		t.Fatalf("warm read %d stats, want one per default view", warmed)
	}
	// A visitor later in the same hour, with the web's own (unrounded) windows, hits every key.
	for _, u := range webDefaultViews(warmAt.Add(17*time.Minute + 23*time.Second)) {
		rec := httptest.NewRecorder()
		stats.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, u, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d: %s", u, rec.Code, rec.Body.String())
		}
	}
	if got := inner.calls.Load(); got != warmed {
		t.Fatalf("%d of the web's default views missed the warmed cache", got-warmed)
	}
}

func TestStatsWarmRefreshesOnlyWhatChanged(t *testing.T) {
	var hits []string
	fail := ""
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits = append(hits, r.URL.Path)
		if r.URL.Path == fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("{}"))
	})
	rev := int64(1)
	w := NewStatsWarm(handler, func(context.Context) (int64, bool) { return rev, true })
	at := time.Date(2026, 10, 27, 12, 10, 0, 0, time.UTC)
	w.now = func() time.Time { return at }
	run := func(want int, label string) {
		t.Helper()
		hits = nil
		err := w.Run(context.Background())
		if fail == "" && err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if len(hits) != want {
			t.Fatalf("%s: %d requests %v, want %d", label, len(hits), hits, want)
		}
	}
	run(11, "first run")
	run(0, "nothing changed")
	at = at.Add(20 * time.Minute) // 12:30, same hour and rolled window
	run(0, "same hour")
	at = at.Add(6 * time.Minute) // 12:36: the rolled windows moved
	run(3, "rolled windows moved")
	at = at.Add(25 * time.Minute) // 13:01: the since-based windows moved
	run(8, "new hour")
	rev++
	fail = "/top-talkers"
	run(11, "new revision")
	fail = ""
	run(1, "the failed view is retried")
	if hits[0] != "/top-talkers" {
		t.Fatalf("retried %v", hits)
	}
}

func TestStatsWarmWaitsForARevision(t *testing.T) {
	called := false
	w := NewStatsWarm(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }),
		func(context.Context) (int64, bool) { return 0, false })
	if err := w.Run(context.Background()); err != nil || called {
		t.Fatalf("ran without a revision: err=%v called=%v", err, called)
	}
	if !strings.HasPrefix(w.Task().Name, "stats") {
		t.Fatalf("task name %q", w.Task().Name)
	}
}

// The revision lookup and the views run under the cached reader's shared locks, so a stalled read
// must give up rather than hold them: every warm call carries a deadline.
func TestStatsWarmCallsHaveDeadlines(t *testing.T) {
	var revisionDeadline, viewDeadline bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, viewDeadline = r.Context().Deadline()
		<-r.Context().Done() // a view that never answers
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	w := NewStatsWarm(handler, func(ctx context.Context) (int64, bool) {
		_, revisionDeadline = ctx.Deadline()
		return 1, true
	})
	w.viewTimeout = 20 * time.Millisecond
	start := time.Now()
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("stalled views reported no error")
	}
	if !revisionDeadline || !viewDeadline {
		t.Fatalf("deadlines: revision=%v view=%v", revisionDeadline, viewDeadline)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("a run of stalled views took %v", took)
	}
}
