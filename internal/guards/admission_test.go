// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package guards

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestGuardsAdmitAfterFinish(t *testing.T) {
	g := New(Options{Enforce: Families})
	ctx, finish := g.Begin(context.Background(), "k", Routes, List)
	finish()
	before := takeSnapshot()
	for _, c := range []context.Context{ctx, context.Background()} {
		tk, err := g.Admit(c)
		if err != nil {
			t.Fatal(err)
		}
		if tk.AcquireTimeout() != 0 {
			t.Errorf("AcquireTimeout = %s, want 0", tk.AcquireTimeout())
		}
		tk.Acquired(context.DeadlineExceeded)
		sctx, cancel := tk.Start(c)
		if _, ok := sctx.Deadline(); ok {
			t.Error("inert ticket set a deadline")
		}
		tk.Finish()
		cancel()
		if n := g.slotsHeld("k"); n != 0 {
			t.Errorf("slotsHeld = %d, want 0", n)
		}
	}
	if !maps.Equal(before, takeSnapshot()) {
		t.Error("an inert ticket moved a counter")
	}
	if g.Begun() != 1 {
		t.Errorf("Begun = %d, want 1", g.Begun())
	}
}

func TestNopGate(t *testing.T) {
	tk, err := NopGate.Admit(context.Background())
	if err != nil || tk.AcquireTimeout() != 0 {
		t.Fatalf("NopGate ticket = %v, %v", tk, err)
	}
	ctx, cancel := tk.Start(context.Background())
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Error("NopGate ticket set a deadline")
	}
}

func TestGuardsForgetIdleClients(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := New(Options{})
		run := func(key string, hold time.Duration) {
			ctx, finish := g.Begin(context.Background(), key, Other, Entity)
			defer finish()
			if err := stmt(ctx, g, 0, hold, nil); err != nil {
				t.Error(err)
			}
		}
		var wg sync.WaitGroup
		for i := range 5000 {
			wg.Go(func() { run(fmt.Sprintf("old-%d", i), 10*time.Millisecond) })
		}
		wg.Wait()
		g.mu.Lock()
		slots := len(g.slots)
		g.mu.Unlock()
		if slots != 0 {
			t.Errorf("len(slots) = %d, want 0", slots)
		}
		time.Sleep(31 * time.Second)
		for i := range 1024 {
			run(fmt.Sprintf("new-%d", i), time.Millisecond)
		}
		g.bmu.Lock()
		budgets := len(g.budgets)
		g.bmu.Unlock()
		if budgets > 1024 {
			t.Errorf("len(budgets) = %d, want <= 1024", budgets)
		}
	})
}
