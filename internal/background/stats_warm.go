// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

const statsWarmInterval = time.Minute

// The revision lookup and the views take the cached reader's shared locks, so a stalled read gives
// up instead of holding them; a view's limit is well past the slowest cold view (~10 s on prod).
const (
	statsWarmRevisionTimeout = 10 * time.Second
	statsWarmViewTimeout     = time.Minute
)

// StatsWarm keeps the all-regions default Analytics views cached. Their keys change when an hour
// is rolled or re-rolled (the revision) and as their windows move, and few visitors look at all
// regions, so nearly every one used to wait seconds on a week of rollups. Each run re-requests,
// through the stats handler itself, only the views whose key changed since the last run.
type StatsWarm struct {
	stats    http.Handler
	revision func(context.Context) (int64, bool)
	now      func() time.Time
	warmed   map[string]int64 // view -> revision it was last filled at

	revisionTimeout, viewTimeout time.Duration
}

// NewStatsWarm warms through stats (the /stats router over the cached reader); revision is the
// revision the cache keys currently carry.
func NewStatsWarm(stats http.Handler, revision func(context.Context) (int64, bool)) *StatsWarm {
	return &StatsWarm{stats: stats, revision: revision, now: time.Now,
		revisionTimeout: statsWarmRevisionTimeout, viewTimeout: statsWarmViewTimeout}
}

// Task checks every minute; a run with nothing changed makes no request.
func (w *StatsWarm) Task() Task {
	return Task{Name: "stats_warm", Interval: statsWarmInterval, Run: w.Run}
}

// Run fills the views whose key changed, one at a time. A view that fails is retried next run.
func (w *StatsWarm) Run(ctx context.Context) error {
	rctx, cancel := context.WithTimeout(ctx, w.revisionTimeout)
	rev, ok := w.revision(rctx)
	cancel()
	if !ok {
		return nil
	}
	var errs []error
	start, filled := time.Now(), 0
	warmed := make(map[string]int64)
	for _, view := range defaultStatsViews(w.now()) {
		if r, done := w.warmed[view]; done && r == rev {
			warmed[view] = rev
			continue
		}
		if ctx.Err() != nil {
			break
		}
		vctx, cancel := context.WithTimeout(ctx, w.viewTimeout)
		req, err := http.NewRequestWithContext(vctx, http.MethodGet, view, nil)
		if err != nil {
			cancel()
			return err
		}
		res := &statusWriter{}
		w.stats.ServeHTTP(res, req)
		cancel()
		if res.status() != http.StatusOK {
			errs = append(errs, fmt.Errorf("warm %s: status %d", view, res.status()))
			continue
		}
		warmed[view] = rev
		filled++
	}
	w.warmed = warmed
	if filled > 0 {
		slog.Info("stats warm: refreshed default views", "component", "background", "views", filled, "revision", rev, "took", time.Since(start).Round(time.Millisecond))
	}
	return errors.Join(errs...)
}

// defaultStatsViews is what beacon-web requests for the Analytics tab's default views (all regions,
// 7-day range) at now. The server rounds every window bound to the hour, so these hour-aligned
// windows land on the same cache keys as any request made within the same hour.
func defaultStatsViews(now time.Time) []string {
	const week = 7 * 24 * time.Hour
	hour := now.UTC().Truncate(time.Hour)
	rolled := now.UTC().Add(-95 * time.Minute).Truncate(time.Hour).Add(time.Hour) // the web's rolledWindow end
	ms := func(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }
	since := ms(hour.Add(-week))
	return []string{
		"/series?since=" + ms(rolled.Add(-24*time.Hour)) + "&until=" + ms(rolled),
		"/series?since=" + ms(rolled.Add(-week)) + "&until=" + ms(rolled),
		"/scopes?since=" + ms(rolled.Add(-week)),
		"/payload-breakdown?since=" + since,
		"/top-nodes?since=" + since + "&limit=10",
		"/top-observers?since=" + since + "&limit=8",
		"/observations?since=" + since,
		"/top-advertisers?since=" + since + "&sort=flood&limit=20",
		"/top-talkers?since=" + since + "&limit=20",
		"/signal?since=" + since + "&until=" + ms(hour),
		"/paths?since=" + since + "&until=" + ms(hour),
	}
}

// statusWriter keeps a response's status and drops its body.
type statusWriter struct {
	code   int
	header http.Header
}

func (s *statusWriter) Header() http.Header {
	if s.header == nil {
		s.header = http.Header{}
	}
	return s.header
}

func (s *statusWriter) Write(p []byte) (int, error) {
	s.WriteHeader(http.StatusOK)
	return len(p), nil
}

func (s *statusWriter) WriteHeader(code int) {
	if s.code == 0 {
		s.code = code
	}
}

func (s *statusWriter) status() int {
	if s.code == 0 {
		return http.StatusOK
	}
	return s.code
}
