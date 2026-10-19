// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package guards

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
)

// stmt runs one fake guarded statement the way the statement router does; wait
// is the acquire wait, hold the database time.
func stmt(ctx context.Context, g Gate, wait, hold time.Duration, acquireErr error) error {
	t, err := g.Admit(ctx)
	if err != nil {
		return err
	}
	time.Sleep(wait)
	t.Acquired(acquireErr)
	if acquireErr != nil {
		return acquireErr
	}
	sctx, cancel := t.Start(ctx)
	defer cancel()
	defer t.Finish()
	select {
	case <-time.After(hold):
		return nil
	case <-sctx.Done():
		return sctx.Err()
	}
}

func handler(g Gate, wait, hold time.Duration, acquireErr error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := stmt(r.Context(), g, wait, hold, acquireErr); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("handler error"))
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
}

type result struct {
	rec *httptest.ResponseRecorder
	at  time.Duration // fake time since the test began
}

// serve runs n concurrent requests and returns their results in start order.
func serve(t0 time.Time, h http.Handler, url, remote string, n int) []result {
	out := make([]result, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			req := httptest.NewRequest("GET", url, nil)
			req.RemoteAddr = remote
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			out[i] = result{rec, time.Since(t0)}
		})
	}
	wg.Wait()
	return out
}

func serveAsync(t0 time.Time, h http.Handler, url, remote string) <-chan result {
	ch := make(chan result, 1)
	go func() { ch <- serve(t0, h, url, remote, 1)[0] }()
	return ch
}

func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		panic(err)
	}
	return m.GetCounter().GetValue()
}

// snapshot reads every guard counter; the registry is process-wide, so tests compare deltas.
type snapshot map[[3]string]float64

func takeSnapshot() snapshot {
	s := snapshot{}
	for _, f := range Families {
		for _, gn := range guardNames {
			s[[3]string{"would", string(f), gn}] = counterValue(metrics.GuardWouldRefuse.WithLabelValues(string(f), gn))
			s[[3]string{"refused", string(f), gn}] = counterValue(metrics.GuardRefused.WithLabelValues(string(f), gn))
		}
	}
	return s
}

func (s snapshot) delta(kind string, f Family, guard string) float64 {
	return takeSnapshot()[[3]string{kind, string(f), guard}] - s[[3]string{kind, string(f), guard}]
}

func expectResult(t *testing.T, r result, status int, retryAfter, body string, at time.Duration) {
	t.Helper()
	if r.rec.Code != status || r.rec.Header().Get("Retry-After") != retryAfter || r.rec.Body.String() != body || r.at != at {
		t.Errorf("got %d Retry-After=%q body=%q at %s; want %d %q %q at %s",
			r.rec.Code, r.rec.Header().Get("Retry-After"), r.rec.Body.String(), r.at, status, retryAfter, body, at)
	}
}

// Observe never changes a response.
func TestGuardsObserveAppliesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := New(Options{Budget: "shadow"})
		before := takeSnapshot()
		t0 := time.Now()
		h := Middleware(g)(handler(g, 6*time.Second, 11*time.Second, nil)) // /routes is list: 10 s
		for _, r := range serve(t0, h, "/api/v1/routes", "192.0.2.1:1", 5) {
			expectResult(t, r, 200, "", "ok", 17*time.Second)
		}
		for guard, want := range map[string]float64{"deadline": 5, "admission": 5, "cap": 1} {
			if d := before.delta("would", Routes, guard); d != want {
				t.Errorf("would_refuse{routes,%s} delta = %v, want %v", guard, d, want)
			}
		}
		for _, guard := range guardNames {
			if d := before.delta("refused", Routes, guard); d != 0 {
				t.Errorf("refused{routes,%s} delta = %v, want 0", guard, d)
			}
		}
		if n := g.slotsHeld("192.0.2.1"); n != 0 {
			t.Errorf("slotsHeld after = %d", n)
		}

		t1 := time.Now()
		hs := Middleware(g)(handler(g, 0, 20*time.Second, nil))
		for _, r := range serve(t1, hs, "/api/v1/stats/paths", "192.0.2.2:1", 2) {
			expectResult(t, r, 200, "", "ok", 20*time.Second)
		}
		if d := before.delta("would", Stats, "budget"); d != 1 {
			t.Errorf("would_refuse{stats,budget} delta = %v, want 1", d)
		}
	})
}

func TestGuardsEnforceRefuses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := New(Options{Enforce: Families, Budget: "shadow"})
		before := takeSnapshot()

		// deadline
		t0 := time.Now()
		r := serve(t0, Middleware(g)(handler(g, 0, 11*time.Second, nil)), "/api/v1/routes", "192.0.2.10:1", 1)[0]
		expectResult(t, r, 503, "5",
			`{"error":{"code":"deadline_exceeded","message":"The request ran past its database time limit. Retry later."}}`, 10*time.Second)
		if ct := r.rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q", ct)
		}

		// cap: four holders on an aggregation (30 s) path, so no deadline cuts them.
		t0 = time.Now()
		long := Middleware(g)(handler(g, 0, 20*time.Second, nil))
		var holders []<-chan result
		for range 4 {
			holders = append(holders, serveAsync(t0, long, "/api/v1/stats/paths", "192.0.2.11:1"))
		}
		synctest.Wait()
		r = serve(t0, long, "/api/v1/stats/paths", "192.0.2.11:1", 1)[0]
		expectResult(t, r, 429, "1",
			`{"error":{"code":"rate_limited","message":"Too many concurrent requests from this client. Retry later."}}`, 5*time.Second)
		for _, ch := range holders {
			expectResult(t, <-ch, 200, "", "ok", 20*time.Second)
		}

		// cap handover: a holder finishing at 3 s hands its slot to the waiter.
		t0 = time.Now()
		holders = holders[:0]
		for range 3 {
			holders = append(holders, serveAsync(t0, long, "/api/v1/stats/paths", "192.0.2.12:1"))
		}
		short := serveAsync(t0, Middleware(g)(handler(g, 0, 3*time.Second, nil)), "/api/v1/stats/paths", "192.0.2.12:1")
		synctest.Wait()
		r = serve(t0, Middleware(g)(handler(g, 0, 4*time.Second, nil)), "/api/v1/stats/paths", "192.0.2.12:1", 1)[0]
		expectResult(t, r, 200, "", "ok", 7*time.Second)
		expectResult(t, <-short, 200, "", "ok", 3*time.Second)
		for _, ch := range holders {
			expectResult(t, <-ch, 200, "", "ok", 20*time.Second)
		}

		// admission
		t0 = time.Now()
		r = serve(t0, Middleware(g)(handler(g, 5*time.Second, 0, context.DeadlineExceeded)), "/api/v1/routes", "192.0.2.13:1", 1)[0]
		expectResult(t, r, 503, "2", `{"error":{"code":"service_unavailable","message":"The server is busy. Retry later."}}`, 5*time.Second)
		ctx, finish := g.Begin(context.Background(), "192.0.2.14", Routes, List)
		tk, err := g.Admit(ctx)
		if err != nil || tk.AcquireTimeout() != 5*time.Second {
			t.Errorf("enforced AcquireTimeout = %v (err %v), want 5s", tk.AcquireTimeout(), err)
		}
		finish()

		for _, c := range []struct {
			f     Family
			guard string
		}{{Routes, "deadline"}, {Stats, "cap"}, {Routes, "admission"}} {
			if d := before.delta("refused", c.f, c.guard); d != 1 {
				t.Errorf("refused{%s,%s} delta = %v, want 1", c.f, c.guard, d)
			}
		}
		after := takeSnapshot()
		var refused float64
		for k, v := range after {
			d := v - before[k]
			switch {
			case k[0] == "refused":
				refused += d
			case k[2] != "budget" && d != 0:
				t.Errorf("would_refuse{%s,%s} moved by %v", k[1], k[2], d)
			}
		}
		if refused != 3 {
			t.Errorf("refused total delta = %v, want 3", refused)
		}

		// ForceObserve: nothing refuses even though every family is listed.
		fo := New(Options{Enforce: Families, ForceObserve: true})
		before = takeSnapshot()
		t0 = time.Now()
		r = serve(t0, Middleware(fo)(handler(fo, 0, 11*time.Second, nil)), "/api/v1/routes", "192.0.2.15:1", 1)[0]
		expectResult(t, r, 200, "", "ok", 11*time.Second)
		if d := before.delta("would", Routes, "deadline"); d != 1 {
			t.Errorf("ForceObserve would_refuse{routes,deadline} delta = %v, want 1", d)
		}
		if d := before.delta("refused", Routes, "deadline"); d != 0 {
			t.Errorf("ForceObserve refused{routes,deadline} delta = %v, want 0", d)
		}

		// SetEnforce(nil) applies to new requests.
		g.SetEnforce(nil)
		t0 = time.Now()
		r = serve(t0, Middleware(g)(handler(g, 0, 11*time.Second, nil)), "/api/v1/routes", "192.0.2.16:1", 1)[0]
		expectResult(t, r, 200, "", "ok", 11*time.Second)
	})
}

// A request that never reaches the database (a cache hit) holds no client slot.
func TestCacheHitTakesNoClientSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := New(Options{Enforce: Families, Budget: "shadow"})
		const remote, key = "192.0.2.20:1", "192.0.2.20"
		t0 := time.Now()
		long := Middleware(g)(handler(g, 0, 20*time.Second, nil))
		var holders []<-chan result
		for range 4 {
			holders = append(holders, serveAsync(t0, long, "/api/v1/stats/paths", remote))
		}
		synctest.Wait()
		if n := g.slotsHeld(key); n != 4 {
			t.Fatalf("slotsHeld = %d, want 4", n)
		}
		cached := Middleware(g)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if n := g.slotsHeld(key); n != 4 {
				t.Errorf("slotsHeld during cache hit = %d, want 4", n)
			}
			_, _ = w.Write([]byte("ok"))
		}))
		for _, r := range serve(t0, cached, "/api/v1/stats/paths", remote, 10) {
			expectResult(t, r, 200, "", "ok", 0)
		}
		if n := g.slotsHeld(key); n != 4 {
			t.Errorf("slotsHeld after cache hits = %d, want 4", n)
		}
		for _, ch := range holders {
			expectResult(t, <-ch, 200, "", "ok", 20*time.Second)
		}
		if n := g.slotsHeld(key); n != 0 {
			t.Errorf("slotsHeld at end = %d, want 0", n)
		}
	})
}

func TestRefusalWriterPassesOtherStatuses(t *testing.T) {
	g := New(Options{Enforce: Families})
	h := Middleware(g)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError) // no refusal: passes through
		_, _ = w.Write([]byte("boom"))
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/routes", nil))
	if rec.Code != 500 || rec.Body.String() != "boom" || rec.Header().Get("Retry-After") != "" {
		t.Errorf("got %d %q", rec.Code, rec.Body.String())
	}
	var _ interface{ Unwrap() http.ResponseWriter } = &refusalWriter{}
}
