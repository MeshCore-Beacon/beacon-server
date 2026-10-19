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
	// freshMinGap is the least time between two runs RunNow starts; inside it RunNow returns the last outcome.
	freshMinGap = 5 * time.Minute
	// freshRunTimeout bounds a run RunNow starts, which outlives the request that asked for it.
	freshRunTimeout = 15 * time.Minute
)

// Planguard runs the read-only plan check, keeps its last report and exports it as metrics.
type Planguard struct {
	run func(context.Context) (*planguard.Report, error)
	now func() time.Time

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

// NewPlanguard returns a Planguard that checks plans with run.
func NewPlanguard(run func(context.Context) (*planguard.Report, error)) *Planguard {
	return &Planguard{run: run, now: time.Now}
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
			_, err := p.do(ctx)
			return err
		},
	}
}

// RunNow runs the check now and returns its report. Concurrent callers share one run, which outlives a caller that
// gives up. Within freshMinGap of the last run's end it returns that run's outcome instead of running again.
func (p *Planguard) RunNow(ctx context.Context) (*planguard.Report, error) {
	p.mu.Lock()
	if p.inflight == nil && !p.attemptAt.IsZero() && p.now().Sub(p.attemptAt) < freshMinGap {
		rep, err := p.last, p.attemptErr
		p.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return rep, nil
	}
	p.mu.Unlock()
	runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), freshRunTimeout)
	ch := make(chan struct{})
	var rep *planguard.Report
	var err error
	go func() {
		defer close(ch)
		defer cancel()
		rep, err = p.do(runCtx)
	}()
	select {
	case <-ch:
		return rep, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Last returns the report of the last run that finished.
func (p *Planguard) Last() (*planguard.Report, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last, p.last != nil
}

// do joins the run in flight, or starts one on ctx, and waits for it.
func (p *Planguard) do(ctx context.Context) (*planguard.Report, error) {
	p.mu.Lock()
	if c := p.inflight; c != nil {
		p.mu.Unlock()
		<-c.done
		return c.rep, c.err
	}
	c := &planguardCall{done: make(chan struct{})}
	p.inflight = c
	p.mu.Unlock()

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
	return rep, err
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
