// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

type seriesReader struct {
	api.Reader
	calls    atomic.Int64
	revision atomic.Int64
	revReads atomic.Int64
	gate     chan struct{} // when set, fetches block until closed
}

func (r *seriesReader) GetStatsSeries(context.Context, time.Time, time.Time, []string) (*api.StatsSeries, error) {
	if r.gate != nil {
		<-r.gate
	}
	return &api.StatsSeries{Revision: r.calls.Add(1)}, nil
}

func (r *seriesReader) AnalyticsRevision(context.Context) (int64, error) {
	r.revReads.Add(1)
	return r.revision.Load(), nil
}

// seriesOnly hides AnalyticsRevision.
type seriesOnly struct{ *seriesReader }

func (seriesOnly) AnalyticsRevision() {}

func TestSeriesCacheKeyedByRevision(t *testing.T) {
	c, _ := newTestClient(t)
	inner := &seriesReader{}
	cr := NewCachedReader(inner, c, CacheTTLs{Stats: time.Hour}).(*CachedReader)
	clock := time.Unix(0, 0)
	cr.rev.now = func() time.Time { return clock }
	get := func(iatas []string) int64 {
		t.Helper()
		got, err := cr.GetStatsSeries(context.Background(), time.UnixMilli(0), time.UnixMilli(3600000), iatas)
		if err != nil {
			t.Fatal(err)
		}
		return got.Revision
	}
	if get(nil) != 1 || get(nil) != 1 || get([]string{""}) != 2 || get([]string{"YVR", "YYJ"}) != 3 || get([]string{"YYJ", "YVR"}) != 3 {
		t.Fatalf("unexpected cache behaviour: %d fetches", inner.calls.Load())
	}
	// A new roll is invisible until the memo expires, then gets a fresh key.
	inner.revision.Store(1)
	if get(nil) != 1 {
		t.Fatal("revision re-read inside the memo window")
	}
	clock = clock.Add(revisionTTL)
	if get(nil) != 4 {
		t.Fatal("new revision did not miss the cache")
	}
	if n := inner.revReads.Load(); n != 2 {
		t.Errorf("revision read %d times, want 2", n)
	}
}

func TestSeriesCacheWithoutRevisionIsUncached(t *testing.T) {
	c, _ := newTestClient(t)
	inner := &seriesReader{}
	cr := NewCachedReader(seriesOnly{inner}, c, CacheTTLs{Stats: time.Hour})
	for range 2 {
		if _, err := cr.GetStatsSeries(context.Background(), time.UnixMilli(0), time.UnixMilli(3600000), nil); err != nil {
			t.Fatal(err)
		}
	}
	if inner.calls.Load() != 2 {
		t.Errorf("fetches = %d, want 2", inner.calls.Load())
	}
}

func TestSeriesCacheSingleflight(t *testing.T) {
	c, _ := newTestClient(t)
	inner := &seriesReader{gate: make(chan struct{})}
	cr := NewCachedReader(inner, c, CacheTTLs{Stats: time.Hour})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := cr.GetStatsSeries(context.Background(), time.UnixMilli(0), time.UnixMilli(3600000), nil); err != nil {
				t.Error(err)
			}
		})
	}
	time.Sleep(50 * time.Millisecond) // let every caller reach the miss
	close(inner.gate)
	wg.Wait()
	if n := inner.calls.Load(); n != 1 {
		t.Errorf("concurrent misses ran %d fetches, want 1", n)
	}
}

// ctxSeriesReader blocks until released and fails if its fetch context was cancelled.
type ctxSeriesReader struct {
	seriesReader
	started chan struct{}
	release chan struct{}
}

func (r *ctxSeriesReader) GetStatsSeries(ctx context.Context, _, _ time.Time, _ []string) (*api.StatsSeries, error) {
	r.calls.Add(1)
	close(r.started)
	<-r.release
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &api.StatsSeries{Hours: []api.StatsSeriesHour{{Status: "complete"}}}, nil
}

func TestSharedFetchSurvivesLeaderCancel(t *testing.T) {
	c, _ := newTestClient(t)
	inner := &ctxSeriesReader{started: make(chan struct{}), release: make(chan struct{})}
	cr := NewCachedReader(inner, c, CacheTTLs{Stats: time.Hour})
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := cr.GetStatsSeries(leaderCtx, time.UnixMilli(0), time.UnixMilli(3600000), nil)
		leaderErr <- err
	}()
	<-inner.started
	waiter := make(chan *api.StatsSeries, 1)
	go func() {
		got, err := cr.GetStatsSeries(context.Background(), time.UnixMilli(0), time.UnixMilli(3600000), nil)
		if err != nil {
			t.Error(err)
		}
		waiter <- got
	}()
	time.Sleep(20 * time.Millisecond) // let the waiter join the in-flight fetch
	cancelLeader()
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Errorf("leader err = %v, want its own cancellation", err)
	}
	close(inner.release)
	if got := <-waiter; got == nil || len(got.Hours) != 1 {
		t.Errorf("waiter got %+v; the leader's cancellation leaked into the shared fetch", got)
	}
	if n := inner.calls.Load(); n != 1 {
		t.Errorf("fetches = %d, want 1", n)
	}
}

func TestSharedFetchWaiterHonoursOwnDeadline(t *testing.T) {
	c, _ := newTestClient(t)
	inner := &ctxSeriesReader{started: make(chan struct{}), release: make(chan struct{})}
	defer close(inner.release)
	cr := NewCachedReader(inner, c, CacheTTLs{Stats: time.Hour})
	go func() {
		_, _ = cr.GetStatsSeries(context.Background(), time.UnixMilli(0), time.UnixMilli(3600000), nil)
	}()
	<-inner.started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := cr.GetStatsSeries(ctx, time.UnixMilli(0), time.UnixMilli(3600000), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the waiter's deadline", err)
	}
	if time.Since(start) > time.Second {
		t.Error("waiter stayed blocked on the leader")
	}
}

func TestSharedFetchReturnsIndependentCopies(t *testing.T) {
	c, _ := newTestClient(t)
	inner := &seriesReader{gate: make(chan struct{})}
	cr := NewCachedReader(inner, c, CacheTTLs{Stats: time.Hour})
	results := make(chan *api.StatsSeries, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			got, err := cr.GetStatsSeries(context.Background(), time.UnixMilli(0), time.UnixMilli(3600000), nil)
			if err != nil {
				t.Error(err)
				return
			}
			got.Since = time.Now().UnixNano() // a handler decorating its own response
			results <- got
		})
	}
	time.Sleep(20 * time.Millisecond)
	close(inner.gate)
	wg.Wait()
	a, b := <-results, <-results
	if a == b {
		t.Fatal("concurrent callers share one response object")
	}
	if inner.calls.Load() != 1 {
		t.Errorf("fetches = %d, want 1", inner.calls.Load())
	}
}
