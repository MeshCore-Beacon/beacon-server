// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"
)

// TestPacedBackfillSlicesPostgres checks that the backfills take their slice and batch widths and floor from the
// store's pacing, and that the result is the same exact table.
func TestPacedBackfillSlicesPostgres(t *testing.T) {
	const minPause = 20 * time.Millisecond
	pace := FillPacing{Duty: 0.5, MinPause: minPause, PacketSitesSlice: 50, CmiBatch: 2}

	f := newPsFixture(t)
	stream := f.packets(40)
	f.unsited(stream...)
	store := New(f.pool, f.pool, 0, 0, WithFillPacing(pace))
	p, ok, err := store.BeginPacketSites(f.ctx)
	if err != nil || !ok {
		t.Fatalf("BeginPacketSites = %v, %v", ok, err)
	}
	t.Cleanup(func() { p.Close(context.Background()) })
	if p.slice != 50 || p.duty != 0.5 || p.pause != minPause {
		t.Fatalf("session pacing slice %d duty %v pause %v", p.slice, p.duty, p.pause)
	}
	if _, err := p.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	st, err := p.Backfill(f.ctx, time.Minute)
	if err != nil || !st.Complete {
		t.Fatalf("Backfill = %+v, %v", st, err)
	}
	n := f.maxID() - 3_000_000_000 // the fixture's ids start above 3,000,000,000
	want := (n + 49) / 50
	if int64(len(p.fills)) != want {
		t.Fatalf("%d fills over %d ids, want %d: %v", len(p.fills), n, want, p.fills)
	}
	for i, r := range p.fills {
		if r[1]-r[0] > 50 || (i > 0 && r[1] != p.fills[i-1][0]) {
			t.Fatalf("fill %d is %v: %v", i, r, p.fills)
		}
	}
	if d := time.Since(start); d < time.Duration(want)*minPause {
		t.Fatalf("%d slices took %v, want at least the floor %v each", want, d, minPause)
	}
	f.wantClean("after the paced backfill")

	c := newCmiFixture(t)
	for i := range 5 {
		h := c.packet(string(rune('a'+i)), 3)
		c.obs(c.pool, h, "YUL", true)
		c.unseeded(cmiBase+int64(1+i), 7, h)
	}
	cs := New(c.pool, c.pool, 0, 0, WithFillPacing(pace))
	s, ok, err := cs.BeginCmi(c.ctx)
	if err != nil || !ok {
		t.Fatalf("BeginCmi = %v, %v", ok, err)
	}
	t.Cleanup(func() { s.Close(context.Background()) })
	if s.batch != 2 || s.duty != 0.5 || s.pause != minPause {
		t.Fatalf("session pacing batch %d duty %v pause %v", s.batch, s.duty, s.pause)
	}
	if _, err := s.Init(c.ctx); err != nil {
		t.Fatal(err)
	}
	if st, err := s.Backfill(c.ctx, time.Minute); err != nil || !st.Complete {
		t.Fatalf("Backfill = %+v, %v", st, err)
	}
	// Five message ids in batches of two: (0,2], (2,4], (4,5].
	if s.fills != 3 {
		t.Fatalf("%d cmi fills, want 3", s.fills)
	}
	if got := c.check(); got.Missing != 0 || got.Extra != 0 {
		t.Fatalf("cmi check = %+v", got)
	}
}
