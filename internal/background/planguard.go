// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
)

const (
	// freshMinGap is the least time between two runs RunNow starts; inside it RunNow returns the last report.
	freshMinGap = 5 * time.Minute
	// freshRunTimeout bounds a run RunNow starts, which outlives the request that asked for it but not the server.
	freshRunTimeout = 15 * time.Minute
)

// Planguard runs the read-only plan check, keeps its last report and exports it as metrics.
type Planguard struct {
	base context.Context // ends at server shutdown; fresh runs derive from it
	run  func(context.Context) (*planguard.Report, error)
	now  func() time.Time

	mu         sync.Mutex
	last       *planguard.Report
	lastAt     time.Time // start of the last run that finished
	attemptAt  time.Time // end of the last run, finished or not
	attemptErr error
	inflight   *planguardCall
}

type planguardCall struct {
	done chan struct{}
	rep  *planguard.Report
	err  error
}

// NewPlanguard returns a Planguard that checks plans with run. base ends every run when it ends (server shutdown).
func NewPlanguard(base context.Context, run func(context.Context) (*planguard.Report, error)) *Planguard {
	return &Planguard{base: base, run: run, now: time.Now}
}

// Task ticks hourly and runs when it has never run or the last run started at least every ago, so the first
// check happens about an hour after boot. A run that fails is retried on the next tick.
func (p *Planguard) Task(every time.Duration) Task {
	return Task{
		Name:     "planguard",
		Interval: time.Hour,
		Run: func(ctx context.Context) error {
			p.mu.Lock()
			due := p.lastAt.IsZero() || p.now().Sub(p.lastAt) >= every
			p.mu.Unlock()
			if !due {
				return nil
			}
			_, err := p.do(ctx, false)
			return err
		},
	}
}

// RunNow runs the check now and returns its report. Concurrent callers share one run, which outlives a caller that
// gives up but not the server. Within freshMinGap of the last run's end it starts nothing: it returns the last
// successful report (its StartedAt gives its age), or the last attempt's error when there is no report.
func (p *Planguard) RunNow(ctx context.Context) (*planguard.Report, error) {
	return p.do(ctx, true)
}

// Last returns the report of the last run that finished.
func (p *Planguard) Last() (*planguard.Report, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last, p.last != nil
}

// do joins the run in flight or starts one, and waits for it or for ctx. A fresh run is bounded by base and
// freshRunTimeout; a scheduled one ends with ctx or base. For fresh callers, the gap is checked under the same lock
// that starts a run, so callers cannot chain runs.
func (p *Planguard) do(ctx context.Context, fresh bool) (*planguard.Report, error) {
	p.mu.Lock()
	c := p.inflight
	if c == nil && fresh && !p.attemptAt.IsZero() && p.now().Sub(p.attemptAt) < freshMinGap {
		rep, err := p.last, p.attemptErr
		p.mu.Unlock()
		if rep != nil {
			return rep, nil
		}
		return nil, err
	}
	if c == nil {
		c = &planguardCall{done: make(chan struct{})}
		p.inflight = c
		var runCtx context.Context
		var cleanup func()
		if fresh {
			var cancel context.CancelFunc
			runCtx, cancel = context.WithTimeout(p.base, freshRunTimeout)
			cleanup = cancel
		} else {
			var cancel context.CancelFunc
			runCtx, cancel = context.WithCancel(ctx)
			stopBase := context.AfterFunc(p.base, cancel)
			cleanup = func() { stopBase(); cancel() }
		}
		go p.start(runCtx, cleanup, c)
	}
	p.mu.Unlock()
	select {
	case <-c.done:
		return c.rep, c.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// start makes one run on ctx and publishes its outcome to c and, when it succeeds, to Last and the metrics.
func (p *Planguard) start(ctx context.Context, cleanup func(), c *planguardCall) {
	defer cleanup()
	start := p.now()
	rep, err := p.run(ctx)
	if err == nil && rep == nil {
		err = errNoReport
	}
	p.mu.Lock()
	p.attemptAt, p.attemptErr = p.now(), err
	if err == nil {
		p.last, p.lastAt = rep, start
		publishPlanguard(rep, p.now())
	} else {
		rep = nil
	}
	c.rep, c.err = rep, err
	p.inflight = nil
	p.mu.Unlock()
	close(c.done)
}

var errNoReport = errors.New("plan check returned no report")

// publishPlanguard replaces the failure series with rep's failing runs, stamps the run time and logs a summary.
func publishPlanguard(rep *planguard.Report, at time.Time) {
	metrics.PlanguardFailures.Reset()
	var failing []string
	for _, r := range rep.Failures() {
		metrics.PlanguardFailures.WithLabelValues(r.ID, r.Mode).Set(1)
		failing = append(failing, r.ID+"|"+r.Mode)
	}
	metrics.PlanguardLastRun.Set(float64(at.UnixNano()) / 1e9)
	msg := fmt.Sprintf("planguard: %d runs, %d failing", len(rep.Results), len(failing))
	if len(failing) > 0 {
		slog.Warn(msg, "component", "background", "failing", failing)
		return
	}
	slog.Info(msg, "component", "background")
}
