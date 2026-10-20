// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
)

// pacedBackfiller is a fakeBackfiller whose batches take delay and that carries a store's fill pacing.
type pacedBackfiller struct {
	*fakeBackfiller
	pace  db.FillPacing
	delay time.Duration
	after func() // called after each batch
}

func (p *pacedBackfiller) FillPacing() db.FillPacing { return p.pace }

func (p *pacedBackfiller) BackfillObservationPayloadTypes(ctx context.Context, from, to int64, maxRows int32) (int64, int64, error) {
	time.Sleep(p.delay)
	n, next, err := p.fakeBackfiller.BackfillObservationPayloadTypes(ctx, from, to, maxRows)
	if p.after != nil {
		p.after()
	}
	return n, next, err
}

func TestPayloadTypeBackfillPacing(t *testing.T) {
	const delay = 40 * time.Millisecond
	timed := func(duty float64) (time.Duration, []idRange) {
		f := &pacedBackfiller{fakeBackfiller: newFakeBackfiller(1, 30, 1), pace: db.FillPacing{Duty: duty}, delay: delay}
		start := time.Now()
		if err := payloadTypeBackfillTask(f, time.Minute, 10, 1000, 3).Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		return time.Since(start), f.takeBatches()
	}
	full, fullBatches := timed(1)
	half, halfBatches := timed(0.5)
	quarter, quarterBatches := timed(0.25)
	// Pacing changes timing only: the same batches in the same order.
	if len(fullBatches) != 3 || !slices.Equal(fullBatches, halfBatches) || !slices.Equal(fullBatches, quarterBatches) {
		t.Fatalf("batches differ: %v / %v / %v", fullBatches, halfBatches, quarterBatches)
	}
	// Duty 1 waits nothing; duty 0.5 waits about each batch's run time after it; duty 0.25 three times it.
	if full >= 6*delay {
		t.Fatalf("duty 1 took %v for three %v batches", full, delay)
	}
	if half < 6*delay {
		t.Fatalf("duty 0.5 took %v, want at least %v", half, 6*delay)
	}
	if quarter < 12*delay {
		t.Fatalf("duty 0.25 took %v, want at least %v", quarter, 12*delay)
	}

	// A cancel during the pause ends the run at once; the batch before it is saved.
	ctx, cancel := context.WithCancel(context.Background())
	f := &pacedBackfiller{fakeBackfiller: newFakeBackfiller(1, 30, 1), pace: db.FillPacing{Duty: 0.5, MinPause: time.Hour}}
	f.after = cancel
	start := time.Now()
	err := payloadTypeBackfillTask(f, time.Minute, 10, 1000, 3).Run(ctx)
	if !errors.Is(err, context.Canceled) || time.Since(start) > 5*time.Second {
		t.Fatalf("cancelled run = %v after %v", err, time.Since(start))
	}
	if b := f.takeBatches(); len(b) != 1 {
		t.Fatalf("batches after cancel %v, want one", b)
	}
	if st := f.saved(t); st.NextID != 11 || st.Done {
		t.Fatalf("state after cancel %+v", st)
	}
}
