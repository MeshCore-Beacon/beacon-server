// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"
)

func TestFillPause(t *testing.T) {
	const minPause = 100 * time.Millisecond
	for _, c := range []struct {
		duty    float64
		elapsed time.Duration
		want    time.Duration
	}{
		{1, 0, minPause},
		{1, 10 * time.Second, minPause}, // duty 1 never waits longer than the floor
		{0.5, 2 * time.Second, 2 * time.Second},
		{0.5, 50 * time.Millisecond, minPause}, // a quick slice still waits the floor
		{0.25, 2 * time.Second, 6 * time.Second},
		{0.25, time.Second, 3 * time.Second},
		{0.1, time.Second, 9 * time.Second},
	} {
		p := FillPacing{Duty: c.duty, MinPause: minPause}
		if got := p.Pause(c.elapsed); got != c.want {
			t.Errorf("duty %v elapsed %v: pause %v, want %v", c.duty, c.elapsed, got, c.want)
		}
	}
	if got := (FillPacing{Duty: 1}).Pause(time.Second); got != 0 {
		t.Errorf("duty 1, no floor: pause %v, want 0", got)
	}
	// An unset duty (a zero FillPacing) means no stretching, never a division by zero.
	if got := (FillPacing{MinPause: minPause}).Pause(time.Second); got != minPause {
		t.Errorf("zero duty: pause %v, want %v", got, minPause)
	}
}

func TestDefaultFillPacing(t *testing.T) {
	want := FillPacing{Duty: 0.5, MinPause: 100 * time.Millisecond, PacketSitesSlice: 500_000, CmiBatch: 10_000}
	if got := DefaultFillPacing(); got != want {
		t.Fatalf("DefaultFillPacing = %+v, want %+v", got, want)
	}
	// A store without the option uses the defaults; with it, the given values.
	if got := New(nil, nil, 0, 0).FillPacing(); got != want {
		t.Fatalf("store default %+v", got)
	}
	custom := FillPacing{Duty: 0.25, MinPause: time.Second, PacketSitesSlice: 100_000, CmiBatch: 2_000}
	if got := New(nil, nil, 0, 0, WithFillPacing(custom)).FillPacing(); got != custom {
		t.Fatalf("store custom %+v", got)
	}
}

func TestPaceFillCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := FillPacing{Duty: 0.5, MinPause: time.Hour}
	done := make(chan bool)
	start := time.Now()
	go func() { done <- p.Wait(ctx, time.Millisecond) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case live := <-done:
		if live {
			t.Fatal("Wait reported a live context after cancel")
		}
		if d := time.Since(start); d > 5*time.Second {
			t.Fatalf("Wait returned %v after cancel", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait ignored the cancel")
	}
	if (FillPacing{Duty: 1}).Wait(ctx, time.Second) {
		t.Fatal("a zero pause on a cancelled context reported live")
	}
	if !(FillPacing{Duty: 1}).Wait(context.Background(), time.Second) {
		t.Fatal("a zero pause on a live context reported cancelled")
	}
}

func TestFillProgress(t *testing.T) {
	now := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	var p FillProgress
	p.now = func() time.Time { return now }
	if p.Due() {
		t.Fatal("due at the first call")
	}
	p.Start(1_000_000)
	now = now.Add(30 * time.Second)
	if p.Due() {
		t.Fatal("due after 30 s")
	}
	now = now.Add(30 * time.Second)
	if !p.Due() {
		t.Fatal("not due after a minute")
	}
	// 250K of 1M ids in a minute: 750K left at the same rate is three minutes.
	if eta, ok := p.Log("packet_sites backfill progress", 250_000, 750_000); !ok || eta != 3*time.Minute {
		t.Fatalf("ETA %v, %v; want 3m", eta, ok)
	}
	if p.Due() {
		t.Fatal("due right after a log")
	}
	now = now.Add(time.Minute)
	if !p.Due() {
		t.Fatal("not due a minute after the last log")
	}
	// No progress since the start: no ETA.
	var q FillProgress
	q.now = func() time.Time { return now }
	q.Start(10)
	now = now.Add(time.Minute)
	if _, ok := q.Log("x", 0, 10); ok {
		t.Fatal("an ETA without progress")
	}
}
